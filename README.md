# Pushary for Haystack

Ask your customer to approve an order on their phone before a Haystack component
executes the action. `PusharyProtectedAction` uses the existing [Pushary Python
SDK](https://github.com/Pushary/pushary-python) for delivery and one-use execution
permits. This package is maintained by Pushary and licensed under MIT.

## Install

Python 3.10+; tested with Haystack 3.1.1 and Pushary 2.1.1. Install the package from PyPI:

```sh
pip install pushary-haystack==0.1.0
```

## Customer phone approval

You need a [Pushary Partner account](https://pushary.com/sign-up?from=agent&plan=partner&utm_source=github&utm_medium=oss-adapter&utm_campaign=pushary-haystack&utm_content=partner-start)
and a customer enrolled through your application's `client.enroll(external_id)`
flow. Keep the full API key on the server in `PUSHARY_API_KEY`.
Use a key for the authenticated tenant, preferably bound to the customer. Never
let a model select tenant, recipient, handler, run ID, call ID, or deadline.

```python
import time
from haystack import Pipeline
from pushary_haystack import PusharyProtectedAction


def release_order(parameters, idempotency_key):
    # Runnable demonstration only. Replace with your idempotent order service.
    return {"order_id": parameters["order_id"], "status": "released"}


operation_config = dict(
    tenant_id="store-1", external_id="customer-1",
    run_id="persisted-order-workflow-1", call_id="release-1",
    action="order.release", target="order-1", revision="release-v1",
    expires_at=int(time.time()) + 3600,  # Persist ONCE, not on each retry.
)
pipeline = Pipeline()
pipeline.add_component("approve", PusharyProtectedAction(release_order, **operation_config))
print(pipeline.run({"approve": {"parameters": {
    "order_id": "order-1", "amount_cents": 1900,
}}}))
```

The action runs inside the component, after approval and successful permit
consumption. Success emits `result`; a refusal emits only `blocked`. Connect
`approve.result` to downstream reporting components. Do not expose an alternative
ungated action tool: this component protects its registered handler, not every
tool or component in an arbitrary agent.

To expose it to a Haystack Agent, wrap the configured component with the native
`ComponentTool`. Its tool schema exposes only `parameters`; recipient and other
trusted configuration stay outside the model's tool arguments:

```python
from haystack.tools import ComponentTool

protected_tool = ComponentTool(
    PusharyProtectedAction(release_order, **operation_config), name="release_order"
)
# Supply protected_tool in your Agent's tools list instead of the raw action.
```

Create a separate component instance for the tool: Haystack does not allow a
component already owned by a pipeline to be wrapped as a `ComponentTool`.

Parameters must be 1–31 flat scalar facts, with keys up to 64 characters and
string values up to 200. The full action question must fit 500 characters.
Nested data, nonfinite numbers, oversized questions, and the reserved
`pushary_binding` key are rejected rather than silently omitted. Include every
effect-relevant argument; do not pass credentials or private customer data.

## Pause, stop the worker, then resume

The default `timeout_seconds=0` creates or looks up the decision and returns
without waiting on the phone. `blocked` does **not** distinguish pending from
terminal refusal. It never authorizes an action. A trusted application can
re-enter the same saved operation after the customer responds; an agent should
not invent a new operation ID to retry a refusal. For a short synchronous wait,
set `timeout_seconds` to a value up to 55.

This is a serialized pipeline/configuration example, not an automatic checkpoint
of every upstream component. Persist the input and `pipeline.dumps()` before
the first invocation, as the demo does, and reload both in a new worker. A larger
pipeline should checkpoint at the protected component using [Haystack's native
breakpoints](https://docs.haystack.deepset.ai/docs/pipeline-breakpoints) so earlier
side effects are not replayed. Load only trusted snapshots and importable handler
code; the API key itself is never serialized. Haystack 3 requires explicitly
allowlisting `pushary_haystack` and your handler's module on `Pipeline.loads`.
The standalone demo allowlists its own `__main__` module, not arbitrary imports.
Keep the same handler revision
and dependencies; a code change requires a new revision and fresh approval.

Clone the repository to run the complete local-order demonstration:

```sh
git clone --branch v0.1.0 https://github.com/Pushary/pushary-haystack.git
cd pushary-haystack
pip install .
# Export PUSHARY_API_KEY securely, then use an enrolled test customer's ID.
python examples/order_approval.py start YOUR_TEST_CUSTOMER_ID
# The process exits. Answer on the customer's phone, then start a new process:
python examples/order_approval.py resume
```

The first command writes `operation.json` exclusively, preserving the original
recipient, IDs, inputs, handler, and expiry. The second reads that exact state.
An approval writes a local SQLite order in `orders.db`; denial does not. Repeated
resumption cannot spend the same permit twice. Use a fresh private directory for
each demonstration. This sends a real decision, but makes no shipment or payment.

## Retry and failure behavior

The binding includes tenant, recipient, run, call, action, target, handler
revision, deadline, and exact parameters. Changed arguments or identity require
a fresh approval. The component copies parameters before asking and checks the
persisted operation deadline again before invoking the handler.

Pushary consumes a durable, one-use permit **before** executing the action.
Duplicate workers, process restarts, or a lost consume response cannot reuse it.
A callback error is recorded as failed and re-raised. A crash after consumption
can leave an uncertain execution; reconcile it against your business service,
never automatically create a replacement approval and replay the action.
Provide business idempotency in the handler using the supplied key. This is
at-most-once permission consumption, not guaranteed exactly-once business effects.

The local operation deadline can shorten approval validity. The decision has a
fixed one-hour lifetime, also enforced by Pushary; its requested lifetime stays
constant on retries to preserve API idempotency. Do not extend a saved deadline.

## Check without an account

```sh
HAYSTACK_TELEMETRY_ENABLED=false python checks/check_component.py
```

This runs the real Haystack pipeline, serialization and published Pushary SDK in
fresh processes. Only the HTTP boundary and business effect are simulated. It
checks approval, rejection, expiry, changed identity/arguments, recipient refusal,
duplicate and concurrent workers, lost responses, and crashes after the effect.
It does not claim live phone delivery was tested. CI repeats it on Python 3.10,
3.12 and 3.13.

Questions or bugs: [GitHub issues](https://github.com/Pushary/pushary-haystack/issues).
