"""Companion probe for the graceful-drain test — proves a draining worker takes
NO new work.

`test_worker_drain.py` tests one half of a warm shutdown: the worker *finishes
the task it's already running*. This DAG tests the other half: once a worker
starts draining, it must *stop pulling new tasks off the queue*.

It fires a `which_worker` task every 15 seconds; each run **occupies the worker for
~12s** (configurable via the `probe_seconds` param), logging a heartbeat every 5s
with the ECS Fargate **task id** of the worker it landed on. Occupying the worker —
rather than returning in under a second — is what makes this a real test: it keeps
the worker continuously busy so there is always work on offer, and it guarantees a
probe is *actively running* when SIGTERM arrives so you can watch what happens to it.

Run it alongside `test_worker_drain` during a deploy.

**Single-worker reading** (the setup this is tuned for):

  - Before the deploy, every probe runs on worker **A** (`ecs-task:A`).
  - The deploy makes ECS start a replacement worker **B** and SIGTERM **A** (warm
    shutdown). A *finishes the probe it is mid-run on*, then stops consuming.
  - Probes fired after that have nowhere to run on A — they **queue** until B is
    healthy, then run on **B** (`ecs-task:B`). You'll see a gap on A, a backlog, and
    then the backlog draining onto B.
  - PROOF of "no new work": after A's SIGTERM, **no probe ever runs on A again** —
    the queue drains onto B instead.
  - FAILURE signal: a probe that logs `⚠️ probe SIGTERM'd mid-run` means A grabbed a
    *new* task and was killed running it — a cold shutdown, not a drain (see PLAN.md
    items 1 and 3).

**With N workers**, the same evidence reads as: the draining worker's task id keeps
appearing *before* its SIGTERM and STOPS appearing *after*, while the other
(non-draining) workers keep running throughout.

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
import signal
import socket
import time
import urllib.request
from datetime import datetime, timedelta, timezone

# Airflow 3 Task SDK, with a fallback for older layouts.
try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # pragma: no cover - Airflow < 3
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

log = logging.getLogger("airflow.task")

DEFAULT_PROBE_SECONDS = 12
HEARTBEAT_SECONDS = 5


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
    params={"probe_seconds": DEFAULT_PROBE_SECONDS},
    tags=["ops", "smoke", "drain-test"],
    doc_md=__doc__,
)
def test_worker_drain_probe():
    @task(retries=0, execution_timeout=timedelta(seconds=60))
    def which_worker() -> dict[str, object]:
        ctx = get_current_context()
        try:
            probe_seconds = int(ctx["params"]["probe_seconds"])
        except (KeyError, TypeError, ValueError):
            probe_seconds = DEFAULT_PROBE_SECONDS

        worker = worker_id()
        start_wall = datetime.now(timezone.utc)
        start = time.monotonic()

        # Catch a COLD shutdown in the act: if this probe is SIGTERM'd mid-run, the
        # worker grabbed a *new* task while draining and is being killed running it —
        # the exact failure this probe exists to detect. Log it loudly, then chain to
        # Airflow's own handler so the task still terminates as Airflow expects.
        original = signal.getsignal(signal.SIGTERM)

        def on_sigterm(signum, frame):
            log.warning(
                "⚠️ probe SIGTERM'd mid-run at t+%.0fs on worker %s — worker did NOT "
                "drain, it consumed a NEW task and is being killed (cold shutdown). "
                "Check PLAN.md items 1 and 3.",
                time.monotonic() - start,
                worker,
            )
            if callable(original):
                original(signum, frame)

        signal.signal(signal.SIGTERM, on_sigterm)

        log.info(
            "🛰️ probe start: occupying worker %s for %ds at %s — a draining worker "
            "should STOP appearing here once it has received SIGTERM",
            worker,
            probe_seconds,
            start_wall.isoformat(),
        )

        beat = 0
        deadline = start + probe_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(HEARTBEAT_SECONDS, remaining))
            beat += 1
            log.info(
                "💓 probe heartbeat %d — t+%.0fs / %ds on worker %s",
                beat,
                time.monotonic() - start,
                probe_seconds,
                worker,
            )

        end_wall = datetime.now(timezone.utc)
        log.info(
            "✅ probe completed full %ds on worker %s — it consumed and ran this task",
            probe_seconds,
            worker,
        )
        return {
            "worker": worker,
            "probe_seconds": probe_seconds,
            "started_at": start_wall.isoformat(),
            "finished_at": end_wall.isoformat(),
        }

    which_worker()


test_worker_drain_probe()
