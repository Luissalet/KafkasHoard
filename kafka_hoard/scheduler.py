"""Background work in two lanes, one job at a time each.

* ``ingest``: watched-folder scans (every ``folders.interval_min``, default 5), mail scans (every ``mail.interval_min``, default 15),
  the optional model pass over documents the rules could not read, and the hourly Phileas sync.
* ``reminders``: the reminder run (every 10 minutes) and housekeeping (hourly).

The lanes, the due-job tick, the duplicate guard and the inline run are the family's (``hoard_link.lanes.LaneScheduler``); this module
says which jobs exist and keeps Kafka's names and status view. A job that raises is logged and recorded, and never stops its lane.
"Scan now" and friends go through the same queues. The time each job last ran is saved (``scheduler.json`` in the data folder), so a
restart does not run every job at once.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Optional, Union

from .hoard_link.lanes import Job, LaneScheduler

TICK_S = 20.0
REMINDERS_S = 600.0
HOUSEKEEPING_S = 3600.0
PHILEAS_S = 3600.0
LANES = ("ingest", "reminders")
JOB_LANE = {"folders": "ingest", "mail": "ingest", "llm": "ingest", "phileas": "ingest", "reminders": "reminders", "housekeeping": "reminders"}
LAST_KINDS = ("folders", "mail", "reminders", "housekeeping", "phileas")


def lane_of(kind: str) -> str:
    return JOB_LANE.get(kind, "ingest")


class Scheduler:
    def __init__(self, engine: Any, store: Any, *, clock: Callable[[], float] = time.time, enabled: bool = True,
                 paused: Callable[[], bool] = lambda: False, folders_interval_min: Callable[[], float] = lambda: 5.0,
                 mail_interval_min: Callable[[], float] = lambda: 15.0, mail_enabled: Callable[[], bool] = lambda: True,
                 phileas_enabled: Callable[[], bool] = lambda: True, state_path: Union[str, Path, None] = None):
        self.engine = engine
        self.store = store
        self.clock = clock
        self.paused = paused
        self.lanes = LaneScheduler({lane: 1 for lane in LANES}, state_path=state_path, enabled=enabled, paused=paused, on_error=self._failed,
                                   clock=clock, tick_s=TICK_S, name="kafka")
        every = {"folders": lambda: max(1.0, folders_interval_min()) * 60, "mail": lambda: max(5.0, mail_interval_min()) * 60,
                 "phileas": lambda: PHILEAS_S, "reminders": lambda: REMINDERS_S, "housekeeping": lambda: HOUSEKEEPING_S}
        enabled_when = {"mail": mail_enabled, "phileas": phileas_enabled}
        for kind in LAST_KINDS:
            job = self._job(kind, "", "schedule")
            job.every_s, job.enabled = every[kind], enabled_when.get(kind)
            self.lanes.register(job)

    # ------------------------------------------------------------------ jobs
    def _call(self, kind: str, ref: str) -> Any:
        if kind == "folders":
            return self.engine.scan_folders(ref)
        if kind == "mail":
            return self.engine.scan_mail()
        if kind == "llm":
            return self.engine.llm_pass(ref)
        if kind == "phileas":
            return self.engine.sync_phileas()
        if kind == "reminders":
            return self.engine.run_reminders()
        if kind == "housekeeping":
            return self.engine.housekeeping()
        raise ValueError(f"unknown job kind {kind}")

    def _job(self, kind: str, ref: str, reason: str) -> Job:
        if kind not in JOB_LANE:
            raise ValueError(f"unknown job kind {kind}")
        return Job(kind, key=f"{kind}:{ref}" if ref else kind, fn=lambda: self._call(kind, ref), lane=lane_of(kind), reason=reason)

    def _failed(self, job: Job, error: BaseException) -> None:
        ref = job.dedupe_key.partition(":")[2]
        try:
            self.store.add_run(job.kind, ref, False, 0, f"{type(error).__name__}: {error}")
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ the lanes
    @property
    def last(self) -> dict[str, float]:
        return self.lanes.last

    @property
    def last_tick_ts(self) -> Optional[float]:
        return self.lanes.last_tick_ts

    @property
    def jobs_done(self) -> int:
        return self.lanes.jobs_done

    def _alive(self) -> bool:
        return self.lanes._alive()

    def start(self) -> None:
        self.lanes.start()

    def stop(self, timeout: float = 5.0) -> None:
        self.lanes.stop(timeout)

    def submit(self, kind: str, ref: str = "", reason: str = "manual") -> Optional[Job]:
        return self.lanes.submit(self._job(kind, ref, reason))

    def run_now(self, kind: str, ref: str = "", timeout: float = 240.0) -> Any:
        """Run a job and wait for its result. Without running lanes (tests, MCP-only mode) it runs inline."""
        return self.lanes.run_now(self._job(kind, ref, "inline" if not self._alive() else "manual"), timeout)

    def enqueue_due(self, now: Optional[float] = None) -> int:
        return self.lanes.enqueue_due(now)

    def status(self) -> dict[str, Any]:
        view = self.lanes.status()

        def current(lane: str) -> Optional[dict[str, str]]:
            jobs = view["lanes"][lane]["current"]
            if not jobs:
                return None
            return {"kind": jobs[0]["kind"], "ref": jobs[0]["key"].partition(":")[2], "reason": jobs[0]["reason"]}

        last = self.lanes.last
        return {"enabled": self.lanes.enabled, "running": view["running"], "paused": bool(self.paused()),
                "lanes": {lane: {"queue": view["lanes"][lane]["queue"], "current": current(lane)} for lane in LANES},
                "last_tick_ts": view["last_tick_ts"], "last_folders_ts": last.get("folders"), "last_mail_ts": last.get("mail"),
                "last_reminders_ts": last.get("reminders"), "last_housekeeping_ts": last.get("housekeeping"), "last_phileas_ts": last.get("phileas"),
                "jobs_done": view["jobs_done"]}
