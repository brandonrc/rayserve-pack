"""Level B demo: a REAL checkmaite capability through the RayJob backend.

Runs checkmaite's DataevalCleaning (image classification) — the lightest
capability: needs only a dataset, ignores models/metrics, and its config has
no required fields. The job pins its own environment via runtimeEnvYAML
(pip: checkmaite + this repo's poc package) — per-job env pinning is the
point being demonstrated: the driver's checkmaite version is irrelevant to
the job's.

The demo ships a tiny synthetic YOLO-classification dataset (2 classes x 4
16x16 PNGs) embedded in the payload and unpacked in-job — demo-only; real
deployments point dataset specs at shared storage.

    python demo/demo_capability.py [--image rayproject/ray:2.43.0-py311] \
        [--pip-ref git+https://github.com/brandonrc/rayserve-pack@main#subdirectory=poc]
"""

from __future__ import annotations

import argparse
import base64
import io
import tarfile
import time
import uuid

import numpy as np

from cm_rayjob import JobPayload, RayJobK8sBackend

NAMESPACE = "geraci-poc"
SCOPE = "geraci-demo-capability"
DATA_ROOT = "/tmp/cm-rayjob-data/dataset"


def tiny_yolo_classification_tar_b64() -> str:
    """2 classes x 4 images of 16x16 random RGB, YOLO-classification layout:
    dataset/test/<class>/<img>.png
    """
    from PIL import Image

    rng = np.random.default_rng(42)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for cls in ("cat", "dog"):
            for i in range(4):
                img = Image.fromarray(
                    rng.integers(0, 255, (16, 16, 3), dtype=np.uint8), "RGB"
                )
                png = io.BytesIO()
                img.save(png, format="PNG")
                png.seek(0)
                info = tarfile.TarInfo(name=f"dataset/test/{cls}/img_{i}.png")
                info.size = len(png.getvalue())
                tar.addfile(info, png)
    return base64.b64encode(buf.getvalue()).decode()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="rayproject/ray:2.43.0-py311")
    parser.add_argument(
        "--pip-ref",
        default="git+https://github.com/brandonrc/rayserve-pack@main#subdirectory=poc",
    )
    args = parser.parse_args()

    backend = RayJobK8sBackend(
        namespace=NAMESPACE,
        scope=SCOPE,
        image=args.image,
        head_memory="3Gi",
        # Per-job env pinning: the job installs ITS OWN checkmaite (and this
        # package for the entrypoint), independent of the driver's versions.
        capability_pip=["checkmaite==0.3.0", args.pip_ref],
        # pip resolution + import of torch-heavy deps takes a while
        active_deadline_seconds=45 * 60,
    )

    jid = str(uuid.uuid4())
    payload = JobPayload(
        job_id=jid,
        scope=SCOPE,
        run_key=jid,
        mode="capability",
        capability_class=(
            "checkmaite.core.image_classification.dataeval_cleaning_capability.DataevalCleaning"
        ),
        task="image_classification",
        datasets=[
            {
                "id": "tiny-demo",
                "dataset_format": "yolo",
                "data_dir": DATA_ROOT,
                "split_folder": "test",
            }
        ],
        analytics_store={"backend": "parquet", "uri": "/tmp/cm-rayjob-analytics"},
        bootstrap_b64=tiny_yolo_classification_tar_b64(),
    )

    print("submitting DataevalCleaning RayJob (pip env resolves in-job)...")
    t0 = time.monotonic()
    job = backend.submit_capability(payload, resources={"num_cpus": 2.0, "memory": "3Gi"})
    print(f"  submitted {job.job_id} as CR in {time.monotonic() - t0:.2f}s")

    ref = job.result(timeout=45 * 60)
    elapsed = time.monotonic() - t0
    print(f"\nCOMPLETED in {elapsed / 60:.1f} min")
    print(f"  run_uid:       {ref.run_uid}")
    print(f"  capability_id: {ref.capability_id}")
    print(f"  store_uri:     {ref.store_uri}")
    print(f"  report:        {'<present>' if ref.report is not None else None}")


if __name__ == "__main__":
    main()
