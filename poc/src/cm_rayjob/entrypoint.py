"""Capability-mode entrypoint — runs INSIDE the RayJob.

Receives a declarative JobPayload, loads models/datasets/metrics from their
spec dicts (mirroring checkmaite-frontend's checkmaite_adapter loaders), and
reuses checkmaite's own execution core (_execute_capability_ref) so run
semantics, analytics-store writes, and CapabilityRunRef construction are
identical to the existing ray backend.

The checkmaite version here is whatever the RayJob's runtimeEnvYAML pinned —
per-job env pinning, independent of the api's version.
"""

from __future__ import annotations

import argparse
import importlib
import json
import time
from typing import Any

from cm_rayjob.payload import JobPayload

SENTINEL = "CM_RAYJOB_RESULT:"


def _import_dotted(path: str) -> Any:
    module_path, _, attr = path.rpartition(".")
    return getattr(importlib.import_module(module_path), attr)


def _load_models(task: str, records: list[dict[str, Any]]) -> list[Any]:
    if task == "object_detection":
        from checkmaite.core.object_detection.models import load_models
    else:
        from checkmaite.core.image_classification.models import load_models
    loaded: list[Any] = []
    for record in records:
        record = dict(record)
        rid = record.pop("id")
        spec = {"model_type": record.pop("resource_type"), **record}
        device = spec.pop("device", None)
        loaded.extend(load_models({rid: spec}, device=device).values())
    return loaded


def _load_datasets(task: str, records: list[dict[str, Any]]) -> list[Any]:
    if task == "object_detection":
        from checkmaite.core.object_detection.dataset_loaders import load_datasets
    else:
        from checkmaite.core.image_classification.dataset_loaders import load_datasets
    specs = {}
    for record in records:
        record = dict(record)
        rid = record.pop("id")
        specs[rid] = {"dataset_type": record.pop("resource_type"), **record}
    return list(load_datasets(specs).values())


def _load_metrics(task: str, records: list[dict[str, Any]]) -> list[Any]:
    if task == "object_detection":
        from checkmaite.core.object_detection import metrics as metric_module
    else:
        from checkmaite.core.image_classification import metrics as metric_module
    loaded = []
    for record in records:
        record = dict(record)
        factory = getattr(metric_module, record.pop("resource_type"))
        loaded.append(factory(**record))
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload-b64", required=True)
    payload = JobPayload.from_b64(parser.parse_args().payload_b64)
    assert payload.mode == "capability", "stub payloads use the inline script"
    assert payload.capability_class, "capability mode requires capability_class"

    from checkmaite.jobs.backends.ray.controller import _execute_capability_ref

    capability = _import_dotted(payload.capability_class)()

    config: Any = payload.config
    config_model = getattr(capability, "config_model", None)
    if config_model is not None and payload.config:
        config = config_model.model_validate(payload.config)

    run_kwargs: dict[str, Any] = {
        "models": _load_models(payload.task, payload.models),
        "datasets": _load_datasets(payload.task, payload.datasets),
        "metrics": _load_metrics(payload.task, payload.metrics),
        "use_cache": False,
        "report_threshold": payload.report_threshold,
        "_analytics_store": payload.analytics_store
        or {"backend": "parquet", "uri": "/tmp/cm-rayjob-analytics"},
    }
    if config:
        run_kwargs["config"] = config

    ref = _execute_capability_ref(capability, run_kwargs)
    print(SENTINEL + json.dumps(ref.model_dump(mode="json")), flush=True)
    # POC result channel is the submitter's log tail; it polls, so give it a
    # beat to catch the final line (production: write to the analytics store).
    time.sleep(4)


if __name__ == "__main__":
    main()
