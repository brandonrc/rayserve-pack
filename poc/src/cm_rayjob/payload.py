"""Serializable job payload — the version-parity fix in one dataclass.

The current checkmaite ray backend cloudpickles live Python objects by
reference, so the api and every Ray worker must run the exact same checkmaite
build. This payload is declarative instead: resource *specs* (the same dicts
the checkmaite-frontend catalog already stores) travel as JSON, and loading
happens inside the job, against whatever checkmaite version the job's own
runtime env pins.
"""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class JobPayload:
    """Everything a RayJob entrypoint needs, JSON-serializable."""

    job_id: str
    scope: str
    run_key: str
    # "stub" (self-contained smoke payload, no checkmaite in the job image)
    # or "capability" (spec-driven checkmaite run).
    mode: str = "stub"

    # --- stub mode ---
    stub_sleep_s: float = 0.0
    stub_fail: bool = False

    # --- capability mode ---
    # Dotted path to the capability class, instantiated with no args.
    capability_class: str | None = None
    # maite task: "image_classification" | "object_detection" — selects the
    # spec loaders, mirroring checkmaite-frontend's checkmaite_adapter.
    task: str = "image_classification"
    # Declarative specs, same shape the frontend catalog stores:
    #   models:   [{"id": ..., "resource_type": ..., **spec}]
    #   datasets: [{"id": ..., "resource_type": ..., **spec}]
    #   metrics:  [{"resource_type": ..., **spec}]
    models: list[dict[str, Any]] = field(default_factory=list)
    datasets: list[dict[str, Any]] = field(default_factory=list)
    metrics: list[dict[str, Any]] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    report_threshold: float = 0.5
    # Forwarded as run_kwargs["_analytics_store"]; workers must reach this URI
    # with their own credentials (same contract as the existing ray backend).
    analytics_store: dict[str, Any] | None = None
    # DEMO ONLY: base64 tar.gz unpacked to /tmp/cm-rayjob-data before loading,
    # so a self-contained demo can ship a tiny dataset without shared storage.
    # Real deployments point dataset specs at storage all jobs can reach.
    bootstrap_b64: str | None = None

    def to_b64(self) -> str:
        return base64.b64encode(json.dumps(asdict(self)).encode()).decode()

    @classmethod
    def from_b64(cls, raw: str) -> JobPayload:
        return cls(**json.loads(base64.b64decode(raw)))
