# PartStream / ApexParts
### A System That Helps Body Shops Get Car Repair Parts Faster

**CMPE 273 — Enterprise Distributed Systems (Fall 2026), Section 01**
**San José State University · Instructor: Rakesh Ranjan**

**Team:** Soumya Atluri · Vignesh Jetty Ravi · Wendy Nguyen · Alicia Kim

---

## Title

**PartStream / ApexParts — A Marketplace for Fast Car Repair Parts**

---

## Abstract

After a car accident, a body shop may need many different parts to repair a car. Sometimes, one missing or late part can delay the whole repair for days or even weeks.

ApexParts is a system that helps body shops get the parts they need on time. When a body shop sends a parts list, the system checks five different sellers at the same time to find available parts.

If a seller cannot deliver a part on time, the system cancels that part from the order and tries another seller. It keeps doing this until it finds a seller that can deliver the part within two days. This process uses the **Saga pattern** to handle changes between different sellers.

The system also uses **Kafka** to send order and delivery information between services. Each part has a deadline based on when it is needed during the repair. This helps the system choose a seller that can deliver the part before the repair needs it.

---

## Business Value

- **Faster repairs:** Body shops can get the parts they need faster, so cars do not have to stay in the shop for weeks.
- **More room in the shop:** Cars waiting for missing parts take up space. Getting parts on time helps body shops use their repair space better.
- **More seller options:** Body shops can check parts from different sellers in one system instead of contacting each seller separately.
- **Lower rental car costs:** Faster repairs can reduce the number of days insurance companies need to pay for rental cars.
- **Better tracking:** Body shops can easily check the status of each part and see when it should arrive.

---

## Main Parts of the System

| Service | What It Does |
|---|---|
| **Order Service** | Takes a parts list from the body shop. Each part has a "needed by" date. |
| **Order Coordinator** | The heart of the project. Asks all sellers, splits the order between the fastest ones, and replaces any part that will be late. |
| **Seller Services (5)** | Five fake sellers built from the same code with different settings. Each has its own database. We can make them slow or crash on purpose. |
| **Delivery Tracker** | Listens to Kafka and records updates like "reserved," "late," "replaced," and "delivered." |
| **Order Status API** | Shows each order and which seller is sending each part (viewed through FastAPI's built-in docs page). |

---

## Proposed Architecture Diagram
<img width="3304" height="2632" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/84b51dd7-c5c7-4bbb-a37e-7acfa470e8c8" />

---

## Project Scope

**What we will build**

- A body shop sends a parts list (we use sample lists instead of real crash photos).
- The coordinator reserves parts from 5 sellers at the same time.
- If a seller is late or fails, the coordinator cancels that part and orders it from another seller.
- Every step is sent as a Kafka event, so the order is never lost even if a service restarts.
- The same message processed twice does not create a double order.
- Tests that turn off sellers or slow them down to show the system still works.

**Extra goals (only if we have time)**

- Use an AI model to find parts from crash photos
- A full shop schedule that plans each repair step
- Try Apache Iggy instead of Kafka
- Monitoring charts with Prometheus and Grafana
- A simple web dashboard

---

## Proposed Technology

| Area | Technology |
|---|---|
| **Services** | Python FastAPI |
| **Talking between services** | REST APIs |
| **Sending messages** | Apache Kafka |
| **Database** | PostgreSQL (one server, a separate database for each service) |
| **Testing** | Python test scripts (send many orders, stop or slow sellers) |
| **Running everything** | Docker Compose |

---

## Running PartStream (Package D: runtime)

Requires Docker with Compose v2.24 or newer. Run every command from the repository root.
Locally, always pass both compose files. `infra/docker-compose.dev.yml` publishes Postgres,
Kafka and each API on `127.0.0.1` only. Topics, keys and producers are listed in
[contracts/events/topics.md](contracts/events/topics.md).

**1. First time only: create your local config**
```bash
cp infra/.env.example infra/.env
```
`infra/.env` is git-ignored. If a port is already taken on your machine (for example a local
PostgreSQL on 5432), change `POSTGRES_HOST_PORT` / `KAFKA_HOST_PORT` there.

**2. Start infrastructure only** (Postgres, Kafka, then create the topics)
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up -d --wait postgres kafka
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml run --rm kafka-init
```
`kafka-init` prints `ok <topic>` for each topic and exits 0. It is safe to re-run.
(Don't rely on `up --wait kafka-init`: it can return before the topics exist.)

**3. Start all application services**
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps up -d --build
```
Apps wait until Postgres is healthy and `kafka-init` has finished. A service whose
`services/<name>/` folder is still a placeholder fails to build. To start only some, name
them: `... --profile apps up -d --build tracker coordinator`.

**4. Stop everything (data is kept)**
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps down
```

**5. Logs and status**
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps logs -f --tail=100 tracker   # omit the name for all
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps ps -a
```

**6. Tracker tests** (no Docker needed; see [services/tracker/README.md](services/tracker/README.md))
```bash
cd services/tracker
python3.12 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
python -m pytest -q
```

**7. Cloud VM: leave out the dev file** so that no Postgres/Kafka/API ports are exposed.
Only the frontend/proxy (port 80) is published.
```bash
docker compose -f infra/docker-compose.yml --profile apps up -d --build
```

**8. Wipe all data**

> ⚠️ **`down -v` permanently deletes the `postgres-data` and `kafka-data` volumes**: every
> database (orders, workflows, inventory, tracker state) and every Kafka topic, message and
> consumer offset. It cannot be undone. Never run it on the cloud VM without a verified backup.
```bash
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps down -v
```
After a wipe, Postgres re-runs `infra/db-init/` on the next start, and step 2 recreates the topics.
