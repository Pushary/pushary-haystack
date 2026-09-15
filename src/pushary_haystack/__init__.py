"""Customer approval at a Haystack component's side-effect boundary."""

import json
import math
import os
import time
from collections.abc import Callable
from typing import Any

from haystack import component, default_from_dict, default_to_dict
from haystack.utils import deserialize_callable, serialize_callable
from pushary.adapters import AdapterKernel, decision_fingerprint, derive_parameters


@component
class PusharyProtectedAction:
    """Run an application-owned action only after spending its human approval.

    Identity and handler come from trusted application state, never model input.
    ``handler(parameters, idempotency_key)`` must implement business idempotency.
    Only ``result`` is emitted on success; refusals emit only ``blocked``.
    """

    def __init__(
        self, handler: Callable[[dict, str], Any], *, tenant_id: str,
        external_id: str, run_id: str, call_id: str, action: str,
        target: str, revision: str, expires_at: int,
        api_key_env: str = "PUSHARY_API_KEY", timeout_seconds: float = 0,
    ):
        config = dict(tenant_id=tenant_id, external_id=external_id, run_id=run_id,
                      call_id=call_id, action=action, target=target, revision=revision,
                      expires_at=expires_at, api_key_env=api_key_env,
                      timeout_seconds=timeout_seconds)
        for name, limit in dict(tenant_id=120, external_id=256, run_id=200,
                                call_id=200, action=100, target=80,
                                revision=200, api_key_env=200).items():
            value = config[name]
            if not isinstance(value, str) or not value.strip() or len(value) > limit:
                raise ValueError(f"{name} must be a nonempty string of at most {limit} characters")
        if type(expires_at) is not int or expires_at <= 0:
            raise ValueError("expires_at must be a persisted Unix timestamp in seconds")
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
                or not 0 <= timeout_seconds <= 55):
            raise ValueError("timeout_seconds must be between 0 and 55")
        if not callable(handler):
            raise ValueError("handler must be callable")
        self._config, self._handler = config, handler

    @component.output_types(result=Any, blocked=str)
    def run(self, parameters: dict) -> dict:
        # Copy before asking: upstream code cannot mutate the approved arguments.
        parameters = derive_parameters(parameters)
        if parameters is None or len(parameters) > 31 or "pushary_binding" in parameters:
            raise ValueError("parameters must contain 1–31 bounded, flat JSON facts; pushary_binding is reserved")
        config = self._config.copy()
        question = f"Approve {config['action']} for {config['target']}? " + json.dumps(
            parameters, sort_keys=True, ensure_ascii=False
        )
        if len(question.encode("utf-16-le")) // 2 > 500:
            raise ValueError("Action and parameters must fit the 500-character approval question")
        remaining = config["expires_at"] - time.time()
        if remaining <= 0:
            return {"blocked": "Operation expired; no action executed."}
        identity = {k: v for k, v in config.items() if k not in ("api_key_env", "timeout_seconds")}
        binding = decision_fingerprint({**identity, "parameters": parameters})
        protect = AdapterKernel("the Haystack component").create_protect(
            api_key=os.environ[config["api_key_env"]], policy=False,
            # Stable across retries: the API includes this TTL in its request hash.
            # The separately persisted operation deadline is checked before execution.
            expires_in_seconds=3600,
            timeout_seconds=config["timeout_seconds"],
        )

        def execute():
            if time.time() >= config["expires_at"]:
                raise TimeoutError("Operation expired while waiting; no action executed")
            return self._handler(parameters.copy(), binding)

        outcome = protect(
            config["action"], execute, external_id=config["external_id"],
            run_id=config["run_id"], call_id=config["call_id"],
            target=config["target"], actor=config["tenant_id"],
            facts={**parameters, "pushary_binding": binding},
            question=question,
        )
        return {"result": outcome.result} if outcome.ok else {"blocked": outcome.reason}

    def to_dict(self) -> dict:
        """Serialize configuration and an importable handler; never the API key."""
        return default_to_dict(self, handler=serialize_callable(self._handler), **self._config)

    @classmethod
    def from_dict(cls, data: dict):
        data = {**data, "init_parameters": dict(data["init_parameters"])}
        data["init_parameters"]["handler"] = deserialize_callable(data["init_parameters"]["handler"])
        return default_from_dict(cls, data)
