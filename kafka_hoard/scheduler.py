"""Background work in two lanes, one job at a time each.

* ``ingest``: watched-folder scans (every ``folders.interval_min``, default 5), mail scans (every ``mail.interval_min``, default 15),
  the optional model pass over documents the rules could not read, and the hourly Phileas sync.
* ``reminders``: the reminder run (every 10 minutes) and housekeeping (hourly).

A job that raises is logged and never stops its lane. "Scan now" and friends go through the same queues.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

log = logging.getLogger("kafka.scheduler")

TICK_S = 20.0
REMINDERS_S = 600.0
HOUSEKEEPING_S = 3600.0
PHILEAS_S = 3600.0
LANES = ("ingest", "reminders")
JOB_LANE = {"folders": "ingest", "mail": "ingest", "llm": "ingest", "phileas": "ingest", "reminders": "reminders", "housekeeping": "reminders"}


@dataclass
class Job:
    kind: str                 # folders | mail | llm | phileas | reminders | housekeeping
    ref: str = ""
    reason: str = "schedule"
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: str = ""


def lane_of(kind: str) -> str:
    return JOB_LANE.get(kind, "ingest")


class Scheduler:
    def __init__(self, engine: Any, store: Any, *, clock: Callable[[], float] = time.time, enabled: bool = True,
                 paused: Callable[[], bool] = lambda: False, folders_interval_min: Callable[[], float] = lambda: 5.0,
                 mail_interval_min: Callable[[], float] = lambda: 15.0, mail_enabled: Callable[[], bool] = lambda: True,
                 phileas_enabled: Callable[[], bool] = lambda: True):
        self.engine = engine
        self.store = store
        self.clock = clock
        self.enabled = enabled
        self.paused = paused
        self.folders_interval_min = folders_interval_min
        self.mail_interval_min = mail_interval_min
        self.mail_enabled = mail_enabled
        self.phileas_enabled = phileas_enabled
        self._queues: dict[str, "queue.Queue[Job]"] = {lane: queue.Queue() for lane in LANES}
        self._pending: set[tuple[str, str]] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._current: dict[str, Optional[Job]] = {lane: None for lane in LANES}
        self.last_tick_ts: Optional[float] = None
        self.last: dict[str, Optional[float]] = {k: None for k in ("folders", "mail", "reminders", "housekeeping", "phileas")}
        self.jobs_done = 0

    def _alive(self) -> bool:
        return any(t.is_alive() for t in self._threads.values())

    def start(self) -> None:
        if self._alive():
            return
        self._stop.clear()
        for lane in LANES:
            thread = threading.Thread(target=self._loop, args=(lane,), name=f"kafka-{lane}", daemon=True)
            self._threads[lane] = thread
            thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        for thread in self._threads.values():
            thread.join(timeout)

    def status(self) -> dict[str, Any]:
        def view(job: Optional[Job]) -> Optional[dict[str, str]]:
            return {"kind": job.kind, "ref": job.ref, "reason": job.reason} if job else None
        return {"enabled": self.enabled, "running": self._alive(), "paused": bool(self.paused()),
                "lanes": {lane: {"queue": self._queues[lane].qsize(), "current": view(self._current[lane])} for lane in LANES},
                "last_tick_ts": self.last_tick_ts, "last_folders_ts": self.last["folders"], "last_mail_ts": self.last["mail"],
                "last_reminders_ts": self.last["reminders"], "last_housekeeping_ts": self.last["housekeeping"], "last_phileas_ts": self.last["phileas"],
                "jobs_done": self.jobs_done}

    def submit(self, kind: str, ref: str = "", reason: str = "manual") -> Optional[Job]:
        key = (kind, ref)
        with self._lock:
            if key in self._pending:
                return None
            self._pending.add(key)
        job = Job(kind, ref, reason)
        self._queues[lane_of(kind)].put(job)
        return job

    def run_now(self, kind: str, ref: str = "", timeout: float = 240.0) -> Any:
        """Run a job and wait for its result. Without running lanes (tests, MCP-only mode) it runs inline."""
        if not self._alive():
            return self._execute(Job(kind, ref, "inline"))
        job = self.submit(kind, ref)
        if job is None:
            return {"queued": True, "note": "already queued"}
        if not job.done.wait(timeout):
            return {"queued": True, "note": "still running; the result will appear shortly"}
        if job.error:
            raise RuntimeError(job.error)
        return job.result

    def _execute(self, job: Job) -> Any:
        if job.kind in self.last:
            self.last[job.kind] = self.clock()
        if job.kind == "folders":
            return self.engine.scan_folders(job.ref)
        if job.kind == "mail":
            return self.engine.scan_mail()
        if job.kind == "llm":
            return self.engine.llm_pass(job.ref)
        if job.kind == "phileas":
            return self.engine.sync_phileas()
        if job.kind == "reminders":
            return self.engine.run_reminders()
        if job.kind == "housekeeping":
            return self.engine.housekeeping()
        raise ValueError(f"unknown job kind {job.kind}")

    def _loop(self, lane: str) -> None:
        while not self._stop.is_set():
            try:
                job = self._queues[lane].get(timeout=1.0)
            except queue.Empty:
                job = None
            if job is not None:
                self._run(job, lane)
                continue
            if lane != "reminders":
                continue
            now = self.clock()
            if self.last_tick_ts is None or now - self.last_tick_ts >= TICK_S:
                self.last_tick_ts = now
                if self.enabled and not self.paused():
                    try:
                        self.enqueue_due(now)
                    except Exception:  # noqa: BLE001
                        log.exception("scheduler tick failed")

    def _run(self, job: Job, lane: str) -> None:
        self._current[lane] = job
        try:
            job.result = self._execute(job)
        except Exception as error:  # noqa: BLE001
            job.error = f"{type(error).__name__}: {error}"
            log.warning("job %s %s failed: %s", job.kind, job.ref, job.error)
            try:
                self.store.add_run(job.kind, job.ref, False, 0, job.error)
            except Exception:  # noqa: BLE001
                pass
        finally:
            with self._lock:
                self._pending.discard((job.kind, job.ref))
            self._current[lane] = None
            self.jobs_done += 1
            job.done.set()

    def _due(self, kind: str, now: float, every_s: float) -> bool:
        last = self.last[kind]
        return last is None or now - last >= every_s

    def enqueue_due(self, now: float) -> int:
        n = 0
        if self._due("folders", now, max(1.0, self.folders_interval_min()) * 60):
            n += bool(self.submit("folders", "", "schedule"))
            self.last["folders"] = now
        if self.mail_enabled() and self._due("mail", now, max(5.0, self.mail_interval_min()) * 60):
            n += bool(self.submit("mail", "", "schedule"))
            self.last["mail"] = now
        if self.phileas_enabled() and self._due("phileas", now, PHILEAS_S):
            n += bool(self.submit("phileas", "", "schedule"))
            self.last["phileas"] = now
        if self._due("reminders", now, REMINDERS_S):
            n += bool(self.submit("reminders", "", "schedule"))
            self.last["reminders"] = now
        if self._due("housekeeping", now, HOUSEKEEPING_S):
            n += bool(self.submit("housekeeping", "", "schedule"))
            self.last["housekeeping"] = now
        return n
