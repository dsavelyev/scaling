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


_final_states = (JobStateType.COMPLETED, JobStateType.CANCELLED,
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

    @staticmethod
    def _serialize_jobids(jobids):
        return b''.join(jobid.encode('utf-8') + b'\n' for jobid in jobids)

    def _update_job_state(self, jobid, state):
        self.jobs[jobid].state = state

        if state.status in _final_states:
            self.jobs.pop(jobid)

    def submit_job(self, spec, callback):
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
        if jobids:
            cancel_args = [self.cancel_cmd]
            out, err, exitcode = self.machine.run_command(
                cancel_args, RemoteScheduler._serialize_jobids(jobids))
            if exitcode != 0:
                raise SchedulerError(f'Cancel command failed with stderr {err}')


def params_to_command(paramdict, args_fmt, site_vars):
    site_params = {name: paramdict[name] for name in site_vars}

    params_stringified = {
        name: param_to_string(value)
        for name, value in paramdict.items()
    }

    args_templates = list(map(Template, args_fmt))

    args = [
        x.substitute(params_stringified).encode('utf-8')
        for x in args_templates
    ]

    return site_params, args


def schedule_jobs(paramdicts, throttles):
    '''
    Generates batches of jobs such that for each (key, value) pair in throttles, the sum
    of values of the parameter `key` in jobspecs never exceeds `value` at any given point
    in time. The jobs that have completed are given to the generator using send().
    '''
    for p in paramdicts:
        if any(throttles.get(k) is not None and p[k] > throttles[k]
               for k in p):
            raise ValueError

    throttle_vars = {}
    for name, value in throttles.items():
        throttle_vars[name] = 0

    batch = []

    for index, paramdict in enumerate(paramdicts):
        while True:
            if all(paramdict[name] + throttle_vars[name] <= value
                   for name, value in throttles.items()):
                batch.append(index)
                for name in throttle_vars:
                    throttle_vars[name] += paramdict[name]
                break  # next job

            else:
                # yield the current batch, ask for the index of a completed job
                finind = yield batch
                batch = []
                findict = paramdicts[finind]
                for name in throttle_vars:
                    throttle_vars[name] -= findict[name]
                continue  # trying to fit this job

    if batch:
        yield batch


class ThrottlingSubmitter(threading.Thread):
    def __init__(self,
                 scheduler,
                 jobspecs,
                 generator,
                 attempt_interval=15,
                 max_attempts_fail_external=10):
        super().__init__()

        self._q = queue.Queue()
        self._evt = threading.Event()
        self._alive_evt = threading.Event()
        self._done = False

        self._schedobj = sched.scheduler()
        self._tpe = ThreadPoolExecutor(max_workers=1)

        self._scheduler = scheduler
        self._jobspecs = jobspecs
        self._generator = generator

        self._attempt_interval = attempt_interval
        self._max_attempts_fail_external = max_attempts_fail_external

        self._batch = []
        self._next_submit_event = None
        self._jobids = {}
        self._jobstates = {}
        self._attempt_counts = [0] * len(jobspecs)
        self._num_pending = len(jobspecs)

        self._results = None

    def stop(self):
        self._fire_event('STOP')

    def result(self):
        if self.is_alive():
            raise RuntimeError('thread is alive')
        return self._results

    def wait_alive(self):
        self._alive_evt.wait()

    def run(self):
        self._alive_evt.set()

        self._batch = queue.deque(self._generator.send(None))
        _logger.debug(f'Received batch {self._batch}')
        self._schedule_next_submit()

        while not self._done:
            self._schedobj.run()
            if not self._done:
                _logger.debug('Scheduler object exhausted but we\'re not done, waiting for events')
                self._evt.wait()
            else:
                _logger.info('Done')

        self._results = {}
        for jobid, jobindex in self._jobids.items():
            self._results[jobindex] = (jobid, self._jobstates[jobindex])

    def _listener(self, jobid, state):
        self._fire_event((jobid, state))

    def _fire_event(self, event):
        _logger.debug(f'Firing event {event}')
        self._schedobj.enter(0, 1, self._handle_event, argument=(event,))
        self._evt.set()

    def _handle_event(self, event):
        _logger.debug(f'Handling event {event}')
        self._evt.clear()

        if event == 'STOP':
            _logger.debug('Stopping')
            if self._next_submit_event is not None:
                _logger.debug('Cancelling next_submit_event')
                self._schedobj.cancel(self._next_submit_event)
                self._next_submit_event = None
            self._done = True
        else:
            jobid, state = event

            index = self._jobids[jobid]
            _logger.info(f'Job {jobid} (aka {index}) entered state {state}')
            self._jobstates[index] = state

            status, exitcode = state.status, state.exit_code
            if status not in _final_states:
                return

            if status == JobStateType.FAIL_EXTERNAL:
                self._attempt_counts[index] += 1
                if self._attempt_counts[index] < self._max_attempts_fail_external:
                    _logger.warning(f'Will resubmit {jobid}')
                    self._batch.append(index)
                else:
                    _logger.error(f'{jobid} failed {self._max_attempts_fail_external} times, will not resubmit')
                return

            _logger.info(f'{jobid} aka {index} done, final state = {state}')
            self._num_pending -= 1
            if not self._num_pending:
                _logger.debug('All jobs done, exiting from handle_event')
                self._done = True
                return

            try:
                newjobs = self._generator.send(index)
                _logger.debug(f'Received batch {newjobs}')
                self._batch.extend(newjobs)
            except StopIteration:
                pass

            self._schedule_next_submit()

    def _schedule_next_submit(self, time=0):
        if self._next_submit_event is not None:
            _logger.debug('Submit event already scheduled')
        elif not self._batch:
            _logger.debug('No jobs to submit, not scheduling')
        else:
            _logger.debug('Scheduling submit')
            self._next_submit_event = self._schedobj.enter(
                time, 0, self._submit)

    def _submit(self):
        self._next_submit_event = None

        index = self._batch.popleft()
        spec = self._jobspecs[index]

        jobid = None

        _logger.info(
            f'Attempting to submit job {index}')

        try:
            jobid = self._scheduler.submit_job(spec, self._listener)
        except SchedulerError as e:
            _logger.error(f'Submit error: {str(e)}')

            self._schedule_next_submit(self._attempt_interval)
            return

        if jobid is not None:
            self._jobids[jobid] = index

        self._schedule_next_submit()

