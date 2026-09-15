"""python examples/order_approval.py start CUSTOMER_ID; then resume after answering."""
import json
import sqlite3
import sys
import time
import uuid
from pathlib import Path

from haystack import Pipeline
from pushary_haystack import PusharyProtectedAction


def release_order(parameters, idempotency_key):
    # Demonstration effect: a local order record, not a shipment or payment.
    with sqlite3.connect("orders.db") as db:
        db.execute("create table if not exists released (id primary key, order_id)")
        db.execute("insert or ignore into released values (?, ?)",
                   (idempotency_key, parameters["order_id"]))
    return {"order_id": parameters["order_id"], "status": "released"}


if __name__ == "__main__":
    path = Path("operation.json")
    if sys.argv[1:] and sys.argv[1] == "start" and len(sys.argv) == 3:
        operation = uuid.uuid4().hex
        pipeline = Pipeline()
        pipeline.add_component("approve", PusharyProtectedAction(
            release_order, tenant_id="demo-store", external_id=sys.argv[2],
            run_id=operation, call_id="release-1", action="order.release",
            target=f"order-{operation[:8]}", revision="demo-v1",
            expires_at=int(time.time()) + 3600,
        ))
        data = {"approve": {"parameters": {"order_id": f"order-{operation[:8]}", "amount_cents": 1900}}}
        with path.open("x") as file:
            json.dump({"pipeline": pipeline.dumps(), "data": data}, file)
    elif sys.argv[1:] != ["resume"]:
        raise SystemExit("Usage: start CUSTOMER_ID | resume (run in a fresh private directory)")
    saved = json.loads(path.read_text())
    # Only load application-owned state. Haystack deserialization imports code.
    pipeline = Pipeline.loads(saved["pipeline"], allowed_modules=["pushary_haystack", "__main__"])
    print(pipeline.run(saved["data"])["approve"])
