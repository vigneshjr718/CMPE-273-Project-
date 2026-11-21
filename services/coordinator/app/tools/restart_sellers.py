"""Controlled loopback HTTP seller; parent-memory ledger survives worker deaths.
This does not claim seller restart persistence or verify C's implementation.
"""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from urllib.parse import unquote
from uuid import uuid4


class RestartSellers:
    def __init__(self, reserve_order, cancel_order):
        self.reserve_order, self.cancel_order = reserve_order, cancel_order
        self.records, self.fences = {}, {}
        self.reserve_calls, self.cancel_calls, self.lookup_calls = [], [], []
        self.reserve_committed, self.cancel_committed, self.release = (
            threading.Event(), threading.Event(), threading.Event())
        self.lock = threading.Lock()
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def reply(self, status, value):
                data=json.dumps(value).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type","application/json")
                    self.send_header("Content-Length",str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):
                    pass
            def do_GET(self):
                self.reply(*fixture.get(unquote(self.path).strip("/").split("/")))
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))))
                status,value,hold=fixture.post(unquote(self.path).strip("/").split("/"),
                                             body,self.headers.get("Idempotency-Key"))
                if hold:
                    hold.set()
                    fixture.release.wait(30)
                self.reply(status,value)
        self.server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,
                                     kwargs={"poll_interval":.05},daemon=True)

    def start(self):
        self.thread.start()
        port=self.server.server_address[1]
        return {sid:f"http://127.0.0.1:{port}/{sid}" for sid in ("seller-1","seller-2")}

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def post(self,path,body,key):
        seller,now=path[0],datetime.now(timezone.utc)
        if path[1:]==["quotes"]:
            return 200,dict(seller_id=seller,part_id=body["part_id"],available_quantity=10,
                estimated_delivery_at=(now+timedelta(hours=int(seller[-1]))).isoformat()),None
        with self.lock:
            if path[1:]==["reservations"]:
                self.reserve_calls.append((body["order_id"],key))
                if key in self.fences:
                    return 409,{"detail":"operation is terminally fenced"},None
                new=key not in self.records
                if new:
                    reply=dict(seller_id=seller,operation_key=key,part_id=body["part_id"],
                        quantity=body["quantity"],status="RESERVED",reservation_id="restart-"+uuid4().hex,
                        estimated_delivery_at=(now+timedelta(hours=1)).isoformat(),reason=None)
                    self.records[key]=(reply,body.copy())
                hold=self.reserve_committed if new and body["order_id"]==self.reserve_order else None
                return 200,self.records[key][0].copy(),hold
            if len(path)==4 and path[1]=="reservation-operations" and path[3]=="cancel":
                original=path[2]
                self.cancel_calls.append((body["order_id"],original,key))
                new=original not in self.fences
                if new:
                    self.fences[original]=dict(seller_id=seller,operation_key=original,
                        cancellation_key=key,part_id=body["part_id"],quantity=body["quantity"],
                        reservation_id=body["reservation_id"],status="CANCELLED",status_version=2,
                        fenced=True,cancellation_receipt="restart-fence-"+uuid4().hex,delivered_at=None)
                hold=self.cancel_committed if new and body["order_id"]==self.cancel_order else None
                return 200,self.fences[original].copy(),hold
        return 404,{"detail":"no such fixture operation"},None

    def get(self,path):
        with self.lock:
            if len(path)==4 and path[1:3]==["reservations","by-key"]:
                key=path[3]
                self.lookup_calls.append(key)
                if key in self.records and key not in self.fences:
                    return 200,self.records[key][0].copy()
            if len(path)==4 and path[1]=="reservations" and path[3]=="delivery":
                saved=next((pair for pair in self.records.values() if pair[0]["reservation_id"]==path[2]),None)
                if saved and saved[0]["operation_key"] not in self.fences:
                    reply,source=saved
                    late=source["order_id"]==self.cancel_order and reply["seller_id"]=="seller-1"
                    needed=datetime.fromisoformat(source["needed_by"].replace("Z","+00:00"))
                    return 200,dict(seller_id=reply["seller_id"],operation_key=reply["operation_key"],
                        reservation_id=reply["reservation_id"],part_id=reply["part_id"],quantity=reply["quantity"],
                        status="IN_TRANSIT" if late else "DELIVERED",status_version=1,
                        estimated_delivery_at=(needed+timedelta(days=1)).isoformat() if late else None,
                        delivered_at=None if late else datetime.now(timezone.utc).isoformat())
        return 404,{"detail":"no observation"}
