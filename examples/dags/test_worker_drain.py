"""Smoke test for graceful Celery-worker draining during an in-place deploy.

This DAG has one long-running task that sleeps for a configurable number of
seconds (default 90) while emitting a heartbeat log every 10s. Use it to prove
the "graceful worker drain" config in PLAN.md actually works:

  1. Trigger this DAG on the dev stack (Airflow UI → Trigger, or
     `airflow exec --stack airflow-<v> -- dags trigger test_worker_drain`).
  2. While the `long_running_sleep` task is running, do an in-place `cdk deploy`
     (or otherwise force a new task-def revision) so ECS rolls the worker service
     and SIGTERMs the old worker.
  3. Watch this task's logs:
       - GOOD (graceful drain): heartbeats continue past the deploy and you see
         "✅ completed full Ns without interruption" — Celery warm-shutdown let
         the task finish before SIGKILL. The task instance ends `success`.
       - BAD (cold shutdown / kill): you see "⚠️ SIGTERM at t+Ns — COLD shutdown"
         and the task ends `failed`/`up_for_retry`. SIGTERM is reaching the task
         process, which means the worker is *not* draining (usually PID-1 / signal
         forwarding — item 1 in PLAN.md).

Each log line identifies the worker by its **ECS Fargate task id** (read from the
`ECS_CONTAINER_METADATA_URI_V4` endpoint) — not the PID, which on Fargate is a
short-lived prefork child (the worker itself is usually PID 1). That task id is
what lets you correlate this DAG's worker against the probe DAG's logs.

Negative check: trigger with a config override of `{"sleep_seconds": 300}` — a
task longer than Fargate's 120s `stopTimeout` cap SHOULD be killed mid-run,
proving `stopTimeout` is the control that matters (and that anything over ~120s
must rely on the retry/adoption safety net, not on draining).

`retries` is deliberately 0 so a kill shows up as a visible failure rather than
being silently re-run — the opposite of the production safety-net setting.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import time
import urllib.request
from datetime import datetime, timezone

# Airflow 3 Task SDK, with a fallback for older layouts.
try:
    from airflow.sdk import dag, get_current_context, task
except ImportError:  # pragma: no cover - Airflow < 3
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

log = logging.getLogger("airflow.task")

DEFAULT_SLEEP_SECONDS = 90
HEARTBEAT_SECONDS = 10


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
    dag_id="test_worker_drain",
    schedule=None,  # manual trigger only — this is an on-demand smoke test
    catchup=False,
    tags=["ops", "smoke", "drain-test"],
    params={"sleep_seconds": DEFAULT_SLEEP_SECONDS},
    doc_md=__doc__,
)
def test_worker_drain():
    @task(retries=0)
    def long_running_sleep() -> dict[str, object]:
        ctx = get_current_context()
        try:
            sleep_seconds = int(ctx["params"]["sleep_seconds"])
        except (KeyError, TypeError, ValueError):
            sleep_seconds = DEFAULT_SLEEP_SECONDS

        worker = worker_id()
        start_wall = datetime.now(timezone.utc)
        start = time.monotonic()

        # Detect a COLD shutdown: if the task process is SIGTERM'd mid-run, the
        # worker did not drain gracefully. Log it loudly, then chain to Airflow's
        # own handler so the task still terminates as Airflow expects.
        original = signal.getsignal(signal.SIGTERM)

        def on_sigterm(signum, frame):
            log.warning(
                "⚠️ SIGTERM at t+%.0fs on worker %s — COLD shutdown: the task process "
                "is being killed, worker did NOT drain gracefully. Check PLAN.md item 1 "
                "(SIGTERM must reach Celery as PID 1).",
                time.monotonic() - start,
                worker,
            )
            if callable(original):
                original(signum, frame)

        signal.signal(signal.SIGTERM, on_sigterm)

        log.info(
            "▶️ start: sleeping %ds on worker %s at %s — trigger an in-place deploy now "
            "to test draining",
            sleep_seconds,
            worker,
            start_wall.isoformat(),
        )

        beat = 0
        deadline = start + sleep_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(HEARTBEAT_SECONDS, remaining))
            beat += 1
            log.info(
                "💓 heartbeat %d — t+%.0fs / %ds on worker %s",
                beat,
                time.monotonic() - start,
                sleep_seconds,
                worker,
            )

        end_wall = datetime.now(timezone.utc)
        log.info(
            "✅ completed full %ds without interruption on worker %s — graceful drain confirmed",
            sleep_seconds,
            worker,
        )
        return {
            "worker": worker,
            "sleep_seconds": sleep_seconds,
            "started_at": start_wall.isoformat(),
            "finished_at": end_wall.isoformat(),
        }

    long_running_sleep()


test_worker_drain()
