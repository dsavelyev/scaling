from concurrent.futures import ThreadPoolExecutor
import enum
import logging
import queue
import sched
import threading
import time

import toolz
import attr
from .util import to_bytes, RepeatingTimerThread, OrderedEnum, param_to_string


_logger = logging.getLogger(__name__)


@enum.unique
class JobStateType(OrderedEnum):
    SUBMITTED = 0
    RUNNING = 1
    COMPLETED = 2
    CANCELLED = 3
    FAIL_EXTERNAL = 4
    FAIL_EXIT_CODE = 5
    UNKNOWN = 1000


final_states = (JobStateType.COMPLETED, JobStateType.CANCELLED,
                JobStateType.FAIL_EXTERNAL, JobStateType.FAIL_EXIT_CODE)


@attr.s(frozen=True)
class JobSpec:
    args = attr.ib()
    site_args = attr.ib()
    cwd = attr.ib()


@attr.s(frozen=True)
class JobState:
    status = attr.ib()
    exit_code = attr.ib()


class Job:
    def __init__(self, jobid, state, callback):
        self.jobid = jobid
        self._state = state
        self._callback = callback
        self._lock = threading.Lock()

    @property
    def state(self):
        with self._lock:
            return self._state

    @state.setter
    def state(self, newstate):
        with self._lock:
            oldstate, self._state = self._state, newstate

        if oldstate != newstate:
            self._callback(self.jobid, newstate)


class SchedulerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return msg


class RemoteScheduler:
    def __init__(self,
                 machine,
                 start_cmd,
                 poll_cmd,
                 cancel_cmd,
                 param_spec,
                 outdir,
                 poll_interval=60):
        self.machine = machine
        self.start_cmd = start_cmd
        self.poll_cmd = poll_cmd
        self.cancel_cmd = cancel_cmd
        self.param_spec = param_spec
        self.outdir = outdir

        self.poll_fails = 0

        self.jobs = {}
        self.lock = threading.Lock()
        self.polling_thread = RepeatingTimerThread(poll_interval,
                                                   self._poll_cb)
        self.closed = False

    def _raise_if_closed(self):
        if self.closed:
            raise RuntimeError('this RemoteScheduler is closed')

    def __enter__(self):
        self._raise_if_closed()

    def __exit__(self, *args):
        self._raise_if_closed()
        self.close()

    def close(self):
        self.polling_thread.stop()

    @staticmethod
    def _serialize_jobids(jobids):
        return b''.join(jobid.encode('utf-8') + b'\n' for jobid in jobids)

    def _update_job_state(self, jobid, state):
        self.jobs[jobid].state = state

        if state.status in final_states:
            self.jobs.pop(jobid)

    def submit_job(self, spec, callback):
        self._raise_if_closed()

        final_args = [self.start_cmd, spec.cwd, self.outdir]
        final_args += map(
            toolz.compose(param_to_string, spec.site_args.__getitem__),
            self.param_spec)
        final_args += spec.args

        out, err, exitcode = self.machine.run_command(final_args)
        if exitcode != 0:
            raise SchedulerError(
                'Submit command failed with exit code {0}.\nStdout:\n{1}\nStderr:\n{2}\n'.
                format(exitcode, out, err))
        else:
            jobid = out.rstrip(b'\r\n').decode('utf-8')

        js = Job(jobid, None, callback)
        with self.lock:
            self.jobs[jobid] = js
        self._update_job_state(jobid, JobState(JobStateType.SUBMITTED, 0))

        return jobid

    def _fail_all(self):
        with self.lock:
            for jobid in self.jobs:
                self._update_job_state(jobid,
                                       JobState(JobStateType.FAIL_EXTERNAL, 0))

    def _poll_cb(self):
        with self.lock:
            if not self.jobs:
                return
            stdin = RemoteScheduler._serialize_jobids(self.jobs)

        poll_args = [self.poll_cmd]
        out, err, exitcode = self.machine.run_command(poll_args, stdin)
        if exitcode != 0:
            self.poll_fails += 1
            if self.poll_fails >= 15:
                _logger.error('Queue poll failed 15 times in a row. '+
                        'Assuming something has gone horribly wrong. '+
                        'Setting all jobs to FAIL_EXTERNAL state')
                self._fail_all()
            return
        else:
            self.poll_fails = 0

        if out:
            for line in out.rstrip(b'\r\n').splitlines():
                jobid, state, exitcode = line.split(b'|')
                exitcode = int(exitcode)
                state = JobStateType[state.decode('utf-8')]

                with self.lock:
                    self._update_job_state(
                        jobid.decode('utf-8'), JobState(state, exitcode))

    def cancel_jobs(self, jobids):
        self._raise_if_closed()

        if jobids:
            cancel_args = [self.cancel_cmd]
            out, err, exitcode = self.machine.run_command(
                cancel_args, RemoteScheduler._serialize_jobids(jobids))
            if exitcode != 0:
                raise SchedulerError(f'Cancel command failed with stderr {err}')
