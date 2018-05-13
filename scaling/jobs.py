import enum
import logging
import threading
import weakref

import toolz
import attr
from .util import RepeatingTimerThread, OrderedEnum, param_to_string


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
    args = attr.ib()       # array of arguments to the program
    site_args = attr.ib()  # array of arguments to the start script
    cwd = attr.ib()


@attr.s(frozen=True)
class JobState:
    status = attr.ib()      # JobStateType
    exit_code = attr.ib()

    def __str__(self):
        return f'{self.status.name}' +\
            (f' with exit code {self.exit_code}' if self.status == JobStateType.FAIL_EXIT_CODE
             else '')


class Job:
    def __init__(self, jobid, state, callback):
        self._jobid = jobid
        self._state = state
        self._callback = callback
        self._lock = threading.Lock()

    @property
    def jobid(self):
        return self._jobid

    @property
    def state(self):
        with self._lock:
            return self._state

    @state.setter
    def state(self, newstate):
        with self._lock:
            oldstate, self._state = self._state, newstate

        if oldstate != newstate:
            self._callback(self._jobid, newstate)


class SchedulerError(Exception):
    pass


class RemoteScheduler:
    '''
    Represents a remote job scheduler accessible via ``machine`` and the three
    ``_cmd`` commands. ``param_spec`` is an array specifying the order of
    parameters to the start script.
    '''

    def __init__(self,
                 machine,
                 start_cmd,
                 poll_cmd,
                 cancel_cmd,
                 param_spec,
                 outdir,
                 poll_interval=60,
                 max_polls_no_job=5):
        self.machine = machine
        self.start_cmd = start_cmd
        self.poll_cmd = poll_cmd
        self.cancel_cmd = cancel_cmd
        self.param_spec = param_spec
        self.outdir = outdir
        self.max_polls_no_job = max_polls_no_job

        self.poll_fails = 0

        self.jobs = {}
        self.ticks = {}
        self.cancelled = set()

        # protects self.jobs
        self.lock = threading.Lock()

        # protects self.cancelled, and also against _poll_cb seeing a cancelled
        # job not yet in self.cancelled
        self.cancel_lock = threading.Lock()

        self.polling_thread = RepeatingTimerThread(poll_interval,
                                                   self._poll_cb)
        self.closed = False

    def _raise_if_closed(self):
        if self.closed:
            raise RuntimeError('this RemoteScheduler is closed')

    def __enter__(self):
        self._raise_if_closed()
        return self

    def __exit__(self, *args):
        self._raise_if_closed()
        self.close()

    def close(self):
        self.polling_thread.stop()

    @staticmethod
    def _serialize_jobids(jobids):
        return b''.join(jobid.encode('utf-8') + b'\n' for jobid in jobids)

    def _update_job_state(self, jobid, state):
        with self.lock:
            js = self.jobs[jobid]()
            if js is None or state.status in final_states:
                self.jobs.pop(jobid)

        if js is not None:
            js.state = state

    def submit_job(self, spec, callback):
        '''
        Submits a job. ``callback`` will be called on the ``Scheduler``'s own
        thread whenever the state of this job changes.
        '''
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

        js = Job(jobid, JobState(JobStateType.SUBMITTED, 0), callback)
        with self.lock:
            self.jobs[jobid] = weakref.ref(js)

        return js

    def _fail_all(self):
        with self.lock:
            jobids = self.jobs.keys().copy()

        for jobid in jobids:
            self._update_job_state(jobid,
                                   JobState(JobStateType.FAIL_EXTERNAL, 0))

    def _poll_cb(self):
        '''
        Called every ``poll_interval`` seconds to poll the job queue.
        '''
        with self.lock:
            if not self.jobs:
                return
            jobids = self.jobs.keys()
            stdin = RemoteScheduler._serialize_jobids(self.jobs)

        poll_args = [self.poll_cmd]
        out, err, exitcode = self.machine.run_command(poll_args, stdin)

        if exitcode != 0:
            self.poll_fails += 1
            if self.poll_fails >= 15:
                _logger.error('Queue poll failed 15 times in a row. '
                              'Assuming something has gone horribly wrong. '
                              'Setting all jobs to FAIL_EXTERNAL state')
                self._fail_all()
            return
        else:
            self.poll_fails = 0

        polled_jobids = set()

        if out:
            for line in out.rstrip(b'\r\n').splitlines():
                jobid, state, exitcode = line.split(b'|')
                jobid = jobid.decode('utf-8')
                state = JobStateType[state.decode('utf-8')]
                exitcode = int(exitcode)

                polled_jobids.add(jobid)

                self._update_job_state(
                    jobid, JobState(state, exitcode))

        missing_jobids = jobids - polled_jobids
        for jobid in missing_jobids:
            # deduce its state:
            try:
                # 1. if the exitcode file exists, the job terminated with an
                # exit code
                exit_code = int(self.machine.get_file(f'{self.outdir}/{jobid}.exitcode'))
            except FileNotFoundError:
                # 2. elif it's in cancelled, it's been cancelled
                with self.cancel_lock:
                    cancelled = jobid in self.cancelled
                    if cancelled:
                        self.cancelled.remove(jobid)

                if cancelled:
                    self._update_job_state(jobid, JobState(JobStateType.CANCELLED, 0))
                    continue

                # 3. else we give it a grace period of ``max_polls_no_job``
                # before considering it FAIL_EXTERNAL
                if jobid not in self.ticks:
                    self.ticks[jobid] = 1
                else:
                    self.ticks[jobid] += 1
                if self.ticks[jobid] >= self.max_polls_no_job:
                    _logger.error(f'Job {jobid}: no exit code after {self.max_polls_no_job} queue '
                                  'polls, assuming FAIL_EXTERNAL')
                    self._update_job_state(jobid, JobState(JobStateType.FAIL_EXTERNAL, 0))
                    self.ticks.pop(jobid)
                else:
                    self._update_job_state(jobid, JobState(JobStateType.SUBMITTED, 0))
            else:
                if jobid in self.ticks:
                    self.ticks.pop(jobid)
                self._update_job_state(jobid, JobState(JobStateType.COMPLETED if exit_code == 0
                                                       else JobStateType.FAIL_EXIT_CODE, exit_code))

    def cancel_jobs(self, jobids):
        self._raise_if_closed()

        if jobids:
            cancel_args = [self.cancel_cmd]

            # Take the lock now to prevent _poll_cb from seeing a state in
            # which the jobs are cancelled, but we don't know yet
            with self.cancel_lock:
                out, err, exitcode = self.machine.run_command(
                    cancel_args, RemoteScheduler._serialize_jobids(jobids))
                if exitcode != 0:
                    raise SchedulerError(f'Cancel command failed with stderr {err}')
                else:
                    self.cancelled.update(jobids)
