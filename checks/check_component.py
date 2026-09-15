"""Offline check: real Haystack + published SDK; only HTTP and the effect are fake."""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

os.environ["HAYSTACK_TELEMETRY_ENABLED"] = "false"
from haystack import Pipeline
from haystack.tools import ComponentTool
from pushary.errors import PusharyError
from pushary_haystack import PusharyProtectedAction


def effect(parameters, key):
    with sqlite3.connect(os.environ["CHECK_DB"]) as db:
        assert db.execute("select count(*) from permits").fetchone()[0] == 1
        db.execute("insert into effects values (?, ?)", (key, json.dumps(parameters)))
    if os.environ.get("CHECK_CRASH"):
        os._exit(23)
    if os.environ.get("CHECK_ERROR"):
        raise RuntimeError("Effect failed after a possible write")
    return parameters


def transport(self, method, path, *, body=None, **kwargs):
    """Model the API's idempotent decision + atomic, bound one-use permit contract."""
    with sqlite3.connect(os.environ["CHECK_DB"], timeout=10) as db:
        if path == "/decisions":
            key = body["idempotencyKey"]
            db.execute("insert or ignore into decisions values (?, ?, 'pending')", (key, json.dumps(body)))
            assert json.loads(db.execute("select body from decisions where id=?", (key,)).fetchone()[0]) == body
            status = db.execute("select status from decisions where id=?", (key,)).fetchone()[0]
            if os.environ.get("CHECK_CREATE_LOST"):
                db.commit()
                raise TimeoutError("Create response lost")
            return dict(decisionId=key, status="answered" if status in ("yes", "no") else status,
                        type="confirm", answered=status in ("yes", "no"), value=status)
        if path == "/authorizations/consume":
            row = db.execute("select body,status from decisions where id=?", (body["authorizationId"],)).fetchone()
            original = json.loads(row[0])
            assert row[1] == "yes"
            for name in ("toolName", "toolTarget", "actor", "externalId", "parameters"):
                assert original[name] == body[name], name
            if os.environ.get("CHECK_WRONG_RECIPIENT"):
                raise PusharyError("Subject mismatch", 409, {"refusal": "subject_mismatch"})
            try:
                db.execute("insert into permits values (?, 'running')", (body["authorizationId"],))
            except sqlite3.IntegrityError:
                raise PusharyError("Already consumed", 409, {"refusal": "already_consumed"})
            if os.environ.get("CHECK_PERMIT_LOST"):
                db.commit()
                raise TimeoutError("Permit response lost")
            if os.environ.get("CHECK_DEADLINE"):
                patch("pushary_haystack.time.time", return_value=time.time() + 7200).start()
            return {"permitId": body["authorizationId"]}
        if path.endswith("/receipt"):
            db.execute("update permits set state=?", (body["outcome"],))
            return {}
        raise AssertionError((method, path))


def worker(config, parameters):
    with patch("pushary.client.PusharyServer._request", transport):
        pipeline = Pipeline()
        pipeline.add_component("approve", PusharyProtectedAction(effect, **config))
        serialized = pipeline.dumps()
        assert "pk_offline.sk_offline" not in serialized
        pipeline = Pipeline.loads(serialized, allowed_modules=["pushary_haystack", "__main__"])
        if os.environ.get("CHECK_TOOL"):
            tool = ComponentTool(PusharyProtectedAction(effect, **config), name="release_order")
            assert set(tool.parameters["properties"]) == {"parameters"}
            return tool.invoke(parameters=parameters)
        return pipeline.run({"approve": {"parameters": parameters}})["approve"]


def main():
    os.environ["PUSHARY_API_KEY"] = "pk_offline.sk_offline"
    config = dict(tenant_id="tenant-1", external_id="customer-1", run_id="run-1",
                  call_id="call-1", action="order.release", target="order-1",
                  revision="release-v1", expires_at=int(time.time()) + 600)
    parameters = {"order_id": "order-1", "amount_cents": 1900}
    with tempfile.TemporaryDirectory() as directory:
        os.environ["CHECK_DB"] = str(Path(directory) / "api.db")
        with sqlite3.connect(os.environ["CHECK_DB"]) as db:
            db.executescript("create table decisions(id primary key, body, status);"
                             "create table permits(id primary key, state);"
                             "create table effects(id primary key, parameters);")

        def sql(query):
            with sqlite3.connect(os.environ["CHECK_DB"]) as db:
                return db.execute(query).fetchall()

        def child(c=config, p=parameters, **env):
            return subprocess.Popen([sys.executable, __file__, json.dumps(c), json.dumps(p)],
                                    env={**os.environ, **env}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def run(c=config, p=parameters, **env):
            proc = child(c, p, **env)
            stdout, stderr = proc.communicate(timeout=30)
            assert proc.returncode == 0, stderr.decode()
            return json.loads(stdout)

        def reset():
            for table in ("decisions", "permits", "effects"):
                sql(f"delete from {table}")

        assert "blocked" in run()  # pending, then another process answers/resumes
        assert not sql("select * from effects")
        assert "blocked" in run() and len(sql("select * from decisions")) == 1
        sql("update decisions set status='yes'")
        assert run()["result"] == parameters
        assert "blocked" in run() and len(sql("select * from effects")) == 1
        assert sql("select state from permits") == [("succeeded",)]

        reset(); assert "blocked" in run(CHECK_TOOL="1")
        sql("update decisions set status='yes'")
        assert run(CHECK_TOOL="1")["result"] == parameters

        for status in ("no", "expired", "cancelled"):
            reset(); run(); sql(f"update decisions set status='{status}'")
            assert "blocked" in run() and not sql("select * from effects")

        for name, value in dict(tenant_id="tenant-2", external_id="customer-2", run_id="run-2",
                                call_id="call-2", action="order.refund", target="order-2",
                                revision="release-v2", expires_at=config["expires_at"] + 1).items():
            reset(); run(); sql("update decisions set status='yes'")
            assert "blocked" in run({**config, name: value})
            assert not sql("select * from effects")
        reset(); run(); sql("update decisions set status='yes'")
        assert "blocked" in run(p={**parameters, "amount_cents": 9900})
        assert not sql("select * from effects")
        assert "blocked" in run({**config, "expires_at": 1})

        reset(); run(); sql("update decisions set status='yes'")
        assert "blocked" in run(CHECK_WRONG_RECIPIENT="1")
        assert not sql("select * from effects")
        processes = [child() for _ in range(2)]
        for proc in processes:
            _, stderr = proc.communicate(timeout=30)
            assert proc.returncode == 0, stderr.decode()
        assert len(sql("select * from effects")) == 1

        for mode, exit_code, count in (("CHECK_CRASH", 23, 1), ("CHECK_ERROR", 1, 1),
                                      ("CHECK_PERMIT_LOST", 0, 0), ("CHECK_DEADLINE", 1, 0)):
            reset(); run(); sql("update decisions set status='yes'")
            proc = child(**{mode: "1"}); proc.communicate(timeout=30)
            assert proc.returncode == exit_code
            assert "blocked" in run()
            assert len(sql("select * from effects")) == count

        reset()
        proc = child(CHECK_CREATE_LOST="1"); proc.communicate(timeout=30)
        assert proc.returncode == 1
        run(); assert len(sql("select * from decisions")) == 1
        for invalid in ({"nested": {}}, {"nan": float("nan")}, {"pushary_binding": "forged"}, {"long": "x" * 201}):
            proc = child(p=invalid); proc.communicate(timeout=30)
            assert proc.returncode == 1
    print("PASS: native Pipeline serialization, fresh-process resumption, approval/refusal/expiry, identity changes, concurrency, and uncertain execution")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(json.dumps(worker(json.loads(sys.argv[1]), json.loads(sys.argv[2]))))
    else:
        main()
