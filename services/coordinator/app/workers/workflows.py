"""Persisted jobs with PostgreSQL order ownership, bounded scans and restart recovery."""
import asyncio
from datetime import timedelta
import logging

from sqlalchemy import select

from app.models import CancellationCheck, DeliveryCheck, LineAttempt, Workflow, WorkflowJob, WorkflowLine
from app.saga.locking import OrderBusy
from app.saga.orchestrator import as_utc

logger = logging.getLogger("coordinator.scheduler")
TERMINAL = {"FULFILLED", "UNFULFILLED", "PARTIALLY_FULFILLED"}


class WorkflowDriver:
    """Called only while the scheduler holds the SAME lock as the manual CLIs."""
    def __init__(self, coordinator, reconciler, delivery, replacement):
        self.c, self.reconciler = coordinator, reconciler
        self.delivery, self.replacement = delivery, replacement

    async def run_locked(self, order_id):
        c = self.c
        lines = await c._initialize(order_id)
        for line in lines:
            async with c.sessions() as session:
                row = await session.get(WorkflowLine, (order_id, line.line_id))
                retry_quotes = row.status == "RECONCILIATION_REQUIRED" and row.reason == "QUOTES_UNAVAILABLE"
            if retry_quotes:
                # _prepare_attempt independently checks all old terminal evidence.
                await c._set_state(order_id, line.line_id, "QUOTING", "line.quoting")
            await c._process_line(order_id, line)
            await self.reconciler._reconcile_line(order_id, line)
            state, _ = await c._line_state(order_id, line.line_id)
            if state == "QUOTING":  # An explicit original rejection was recovered.
                await c._process_line(order_id, line)
        requests = await self.delivery._claim(order_id)
        results = await asyncio.gather(*[self.delivery._fetch(r) for r in requests], return_exceptions=True)
        for request, result in zip(requests, results):
            if isinstance(result, BaseException):
                raise result
            await self.delivery._record(request, *result)
        for line in lines:
            state, _ = await c._line_state(order_id, line.line_id)
            # Unknown writes reconcile within their saved budget; never silently
            # turn them into cancellation/replacement decisions automatically.
            if state in {"RESERVED", "CANCELLATION_PENDING"}:
                request = await self.replacement._begin(order_id, line)
                if request:
                    await self.replacement._record(request, *await self.replacement._fetch(request))
            state, _ = await c._line_state(order_id, line.line_id)
            if state == "REPLACING":
                await self.replacement._resume(order_id, line)


class WorkflowScheduler:
    def __init__(self, coordinator, driver, *, batch_size=20, concurrency=3,
                 order_timeout_seconds=30, retry_seconds=2, max_failures=5):
        if (not 1 <= batch_size <= 100 or not 1 <= concurrency <= 10
                or order_timeout_seconds <= 0 or not 0 < retry_seconds <= 60
                or not 1 <= max_failures <= 20):
            raise ValueError("invalid scheduler limits")
        self.c, self.driver, self.batch_size = coordinator, driver, batch_size
        self.concurrency, self.timeout, self.retry = concurrency, order_timeout_seconds, retry_seconds
        self.max_failures = max_failures

    def _backoff(self, failures):
        return min(60, self.retry * 2 ** min(max(failures - 1, 0), 8))

    async def _claim(self, order_id):
        async with self.c.sessions.begin() as session:
            job = await session.get(WorkflowJob, order_id, with_for_update=True)
            if job is None or job.next_run_at is None or as_utc(job.next_run_at) > self.c.clock():
                return False
            job.status, job.last_started_at = "RUNNING", self.c.clock()
            job.run_count += 1
            # A live owner holds the advisory lock. After process death the saved
            # schedule becomes due without any in-memory lease/queue reconstruction.
            job.next_run_at = self.c.clock() + timedelta(seconds=self.timeout + self.retry)
            return True

    async def _finish(self, order_id):
        c, due = self.c, []
        now = c.clock()
        async with c.sessions.begin() as session:
            job = await session.get(WorkflowJob, order_id, with_for_update=True)
            workflow = await session.get(Workflow, order_id)
            rows = list(await session.scalars(select(WorkflowLine).where(WorkflowLine.order_id == order_id)))
            quote_failure = any(row.reason == "QUOTES_UNAVAILABLE" for row in rows)
            job.failure_count = job.failure_count + 1 if quote_failure else 0
            for row in rows:
                attempt = await session.get(LineAttempt, row.current_attempt_id) if row.current_attempt_id else None
                if row.status in {"PENDING", "QUOTING", "RESERVING", "REPLACING"}:
                    due.append(now)
                elif row.status in {"UNKNOWN", "RECONCILING"}:
                    due.append(as_utc(attempt.next_retry_at) if attempt and attempt.next_retry_at else now)
                elif row.status == "RESERVED":
                    check = await session.get(DeliveryCheck, row.current_attempt_id)
                    due.append(as_utc(check.next_poll_at) if check and check.next_poll_at else now)
                elif row.status == "CANCELLATION_PENDING":
                    check = await session.get(CancellationCheck, row.current_attempt_id)
                    if check and check.status == "PENDING" and check.next_retry_at:
                        due.append(as_utc(check.next_retry_at))
                elif row.status == "RECONCILIATION_REQUIRED" and row.reason == "QUOTES_UNAVAILABLE":
                    due.append(now + timedelta(seconds=self._backoff(job.failure_count)))
            job.last_finished_at = now
            if workflow.status in TERMINAL:
                job.status, job.next_run_at, job.last_error = "COMPLETE", None, None
            elif due:
                job.status, job.next_run_at = "WAITING", min(due)
                job.last_error = "QUOTES_UNAVAILABLE" if quote_failure else None
            else:
                job.status, job.next_run_at, job.last_error = "PARKED", None, "MANUAL_REVIEW_REQUIRED"
            return job.status.lower()

    async def _failed(self, order_id, error):
        async with self.c.sessions.begin() as session:
            job = await session.get(WorkflowJob, order_id, with_for_update=True)
            job.failure_count += 1
            job.status, job.last_error = "WAITING", type(error).__name__[:80]
            job.last_finished_at = self.c.clock()
            if job.failure_count >= self.max_failures:
                job.status, job.next_run_at = "PARKED", None
            else:
                job.next_run_at = self.c.clock() + timedelta(seconds=self._backoff(job.failure_count))
        logger.error("workflow_job_deferred", extra={"order_id": order_id, "failure_type": type(error).__name__})

    async def run_order(self, order_id):
        try:
            async with self.c.lock(order_id):
                if not await self._claim(order_id):
                    return "not_due"
                try:
                    async with asyncio.timeout(self.timeout):
                        await self.driver.run_locked(order_id)
                    return await self._finish(order_id)
                except Exception as error:
                    await self._failed(order_id, error)
                    return "failed"
                # Cancellation/process death intentionally leaves the committed
                # RUNNING claim and current attempt for a later process to resume.
        except OrderBusy:
            return "busy"

    async def run_once(self, *, order_id=None, stop=None):
        async with self.c.sessions() as session:
            query = select(WorkflowJob.order_id).where(
                WorkflowJob.next_run_at <= self.c.clock()).order_by(
                    WorkflowJob.next_run_at, WorkflowJob.order_id).limit(self.batch_size)
            if order_id is not None:
                query = query.where(WorkflowJob.order_id == order_id)
            ids = list(await session.scalars(query))
        semaphore = asyncio.Semaphore(self.concurrency)
        async def process(oid):
            async with semaphore:
                if stop is not None and stop.is_set():
                    return "stopped"
                return await self.run_order(oid)
        results = await asyncio.gather(*[process(oid) for oid in ids])
        summary = {"selected": len(ids)}
        for result in results:
            summary[result] = summary.get(result, 0) + 1
        return summary
