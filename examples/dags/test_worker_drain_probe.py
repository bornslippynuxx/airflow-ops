"""Companion probe for the graceful-drain test — proves a draining worker takes
NO new work.

`test_worker_drain.py` tests one half of a warm shutdown: the worker *finishes
the task it's already running*. This DAG tests the other half: once a worker
starts draining, it must *stop pulling new tasks off the queue*.

It fires a tiny task (`which_worker`) every 15 seconds; each run logs the ECS
Fargate **task id** of the worker it landed on. Run it alongside
`test_worker_drain` during a deploy:

  - Note the worker (task id) that `test_worker_drain.long_running_sleep` is on,
    and the moment it logs receiving (or gracefully not receiving) SIGTERM.
  - In this probe's logs, that task id should keep appearing *before* the SIGTERM
    and then STOP appearing *after* it — new probes land only on the other
    (non-draining) workers, which keep running throughout. That's the proof the
    draining worker consumes no new messages.
  - If the draining worker's task id keeps showing up on probes fired after its
    SIGTERM, it is still consuming — the warm shutdown isn't taking effect (see
    PLAN.md items 1 and 3).

The worker is identified by its ECS Fargate task id (read from the
`ECS_CONTAINER_METADATA_URI_V4` endpoint), not the PID — on Fargate the worker is
usually PID 1 and a task's os.getpid() is a short-lived prefork child, so neither
distinguishes workers.

⚠️ Paused by default (`is_paused_upon_creation=True`). A DAG that runs every 15s
floods the metadata DB, so **unpause it only for the test window and pause it again
afterward** (this is exactly the metadata churn `metadata-clean` exists to purge).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import urllib.request
from datetime import datetime, timedelta, timezone

# Airflow 3 Task SDK, with a fallback for older layouts.
try:
    from airflow.sdk import dag, task
except ImportError:  # pragma: no cover - Airflow < 3
    from airflow.decorators import dag, task

log = logging.getLogger("airflow.task")


def worker_id() -> str:
    """Best-effort stable identity of the ECS Fargate task running this worker.

    On Fargate the celery worker is usually PID 1 and a task's own os.getpid() is a
    short-lived prefork child, so neither identifies the worker. The ECS task
    metadata endpoint returns the Task ARN, whose last segment uniquely names the
    Fargate task (= the worker instance). Falls back to the hostname off-Fargate.
    """
    base = os.environ.get("ECS_CONTAINER_METADATA_URI_V4") or os.environ.get("ECS_CONTAINER_METADATA_URI")
    if base:
        try:
            with urllib.request.urlopen(f"{base}/task", timeout=2) as resp:  # noqa: S310 - trusted link-local endpoint
                task_arn = json.load(resp).get("TaskARN", "")
            task_short = task_arn.rsplit("/", 1)[-1] if task_arn else ""
            if task_short:
                return f"ecs-task:{task_short}"
        except Exception as e:  # noqa: BLE001 - identity is best-effort, never fail the task over it
            log.warning("could not read ECS task metadata (%s); falling back to hostname", e)
    return f"host:{socket.gethostname()}"


@dag(
    dag_id="test_worker_drain_probe",
    schedule=timedelta(seconds=15),  # a fresh run (and task) every 15s
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,  # start from "now", don't backfill the gap since start_date
    max_active_runs=16,  # let runs overlap so the 15s cadence isn't throttled to 1
    dagrun_timeout=timedelta(minutes=5),
    is_paused_upon_creation=True,  # OFF by default — see the docstring warning
    tags=["ops", "smoke", "drain-test"],
    doc_md=__doc__,
)
def test_worker_drain_probe():
    @task(retries=0, execution_timeout=timedelta(seconds=30))
    def which_worker() -> dict[str, str]:
        worker = worker_id()
        now = datetime.now(timezone.utc)
        log.info(
            "🛰️ probe ran on worker %s at %s — a draining worker should STOP appearing "
            "here once it has received SIGTERM",
            worker,
            now.isoformat(),
        )
        return {"worker": worker, "ran_at": now.isoformat()}

    which_worker()


test_worker_drain_probe()
