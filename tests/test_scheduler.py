"""The scheduler: due jobs, lanes, failures, pause, inline runs."""

from __future__ import annotations

import time

from kafka_hoard.scheduler import HOUSEKEEPING_S, REMINDERS_S, Scheduler, lane_of


class FakeEngine:
    def __init__(self):
        self.calls = []

    def scan_folders(self, ref=""):
        self.calls.append(("folders", ref))
        return {"files": 0}

    def scan_mail(self):
        self.calls.append(("mail", ""))
        return {"ok": True}

    def llm_pass(self, ref):
        self.calls.append(("llm", ref))
        raise RuntimeError("model fell over")

    def sync_phileas(self):
        self.calls.append(("phileas", ""))
        return {"ok": True}

    def run_reminders(self):
        self.calls.append(("reminders", ""))
        return {"sent": 0}

    def housekeeping(self):
        self.calls.append(("housekeeping", ""))
        return {}


class FakeStore:
    def __init__(self):
        self.runs = []

    def add_run(self, *a):
        self.runs.append(a)


class Now:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def make(**kw):
    engine, store = FakeEngine(), FakeStore()
    kw.setdefault("clock", Now())
    return Scheduler(engine, store, **kw), engine, store


def test_lanes():
    assert lane_of("folders") == lane_of("mail") == lane_of("llm") == lane_of("phileas") == "ingest"
    assert lane_of("reminders") == lane_of("housekeeping") == "reminders"


def test_enqueue_due_submits_every_kind_once_then_waits():
    s, _, _ = make()
    assert s.enqueue_due(1000.0) == 5
    assert s.enqueue_due(1010.0) == 0                             # still queued: not queued twice
    assert s.status()["lanes"]["ingest"]["queue"] == 3 and s.status()["lanes"]["reminders"]["queue"] == 2


def test_after_a_run_a_job_is_due_again_only_when_its_interval_has_passed():
    clock = Now()
    s, _, _ = make(clock=clock)
    for kind in ("folders", "mail", "phileas", "reminders", "housekeeping"):
        s.run_now(kind)                                           # the lanes are not running: inline
    clock.t += REMINDERS_S + 1
    assert s.enqueue_due(clock()) == 2 and s.last["housekeeping"] == 1000.0         # folders (5 min) and reminders (10 min); mail, Phileas and housekeeping wait
    assert HOUSEKEEPING_S > REMINDERS_S


def test_mail_and_phileas_follow_their_switches():
    s, _, _ = make(mail_enabled=lambda: False, phileas_enabled=lambda: False)
    assert s.enqueue_due(1000.0) == 3          # folders, reminders, housekeeping


def test_intervals_come_from_the_callbacks():
    clock = Now(0.0)
    interval = {"min": 1.0}
    s, _, _ = make(clock=clock, folders_interval_min=lambda: interval["min"])
    s.run_now("folders")
    folders = s.lanes._registered["folders"]
    clock.t = 59.0
    assert not s.lanes.is_due(folders, clock())
    clock.t = 61.0
    assert s.lanes.is_due(folders, clock())
    interval["min"] = 10.0                                        # a setting changed at runtime
    assert not s.lanes.is_due(folders, clock())
    interval["min"] = 0.0                                         # never faster than a minute
    assert s.lanes.is_due(folders, clock())


def test_a_duplicate_submission_is_ignored_while_pending():
    s, _, _ = make()
    assert s.submit("folders") is not None and s.submit("folders") is None and s.submit("folders", "/x") is not None


def test_an_unknown_job_kind_is_refused():
    s, _, _ = make()
    try:
        s.submit("pigeons")
        raised = False
    except ValueError:
        raised = True
    assert raised


def test_the_time_of_the_last_runs_survives_a_restart(tmp_path):
    state = tmp_path / "scheduler.json"
    clock = Now(5000.0)
    first, _, _ = make(clock=clock, state_path=state)
    first.run_now("folders")
    first.run_now("mail")
    clock.t = 5100.0
    second, _, _ = make(clock=clock, state_path=state)
    assert second.last["folders"] == 5000.0 and second.last["mail"] == 5000.0
    assert second.enqueue_due(clock()) == 3                       # reminders, housekeeping and Phileas never ran; folders and mail are not due


def test_run_now_is_inline_when_the_lanes_are_not_running():
    s, engine, _ = make()
    assert s.run_now("folders", "/x") == {"files": 0} and engine.calls == [("folders", "/x")]
    assert s.last["folders"] is not None


def test_run_now_through_the_running_lanes_and_failures_are_recorded():
    s, engine, store = make(enabled=False)
    s.start()
    try:
        assert s.run_now("mail", timeout=10) == {"ok": True}
        try:
            s.run_now("llm", "d_1", timeout=10)
            raised = False
        except RuntimeError as exc:
            raised = "model fell over" in str(exc)
        assert raised and store.runs and store.runs[-1][0] == "llm" and store.runs[-1][2] is False
        assert s.run_now("reminders", timeout=10) == {"sent": 0}       # the lane survived the failure
        assert s.status()["running"] and s.status()["jobs_done"] >= 3
    finally:
        s.stop()
    assert not s.status()["running"]


def test_pause_and_disable_stop_the_schedule_but_not_manual_runs():
    paused = {"v": True}
    s, engine, _ = make(paused=lambda: paused["v"], enabled=True)
    assert s.status()["paused"]
    s.start()
    try:
        time.sleep(1.3)
        assert engine.calls == []
        assert s.run_now("folders", timeout=10) == {"files": 0}
    finally:
        s.stop()
