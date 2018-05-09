import fnmatch
import logging
import os.path
import queue
import sched
from string import Template
import threading
import time

import attr
from .jobs import JobSpec, JobState, JobStateType, SchedulerError, final_states
from .grok import Grok
from .machine import SSHMachine
from .util import poll_keyboard, handle_sigint


_logger = logging.getLogger(__name__)


@attr.s(frozen=True)
class MachineSpec:
    host = attr.ib()
    username = attr.ib()
    port = attr.ib()


@attr.s(frozen=True)
class LaunchProfile:
    machine = attr.ib()
    start_cmd = attr.ib()
    poll_cmd = attr.ib()
    cancel_cmd = attr.ib()
    base_dir = attr.ib()
    params = attr.ib()
    param_order = attr.ib()
    out_file_specs = attr.ib()


@attr.s(frozen=True)
class ProgSpec:
    args = attr.ib()
    params = attr.ib()
    stdout = attr.ib()
    out_file_specs = attr.ib()
    in_file_specs = attr.ib()


@attr.s(frozen=True)
class OutputSpec:
    vartypes = attr.ib()
    regex = attr.ib()


@attr.s(frozen=True)
class InFileSpec:
    name = attr.ib()
    template = attr.ib()


@attr.s(frozen=True)
class OutFileSpec:
    name = attr.ib()
    outputspecs = attr.ib()


@attr.s(frozen=True)
class InFileCreationSpec:
    name = attr.ib()
    contents = attr.ib()


@attr.s(frozen=True)
class LaunchSpec:
    args = attr.ib()
    params = attr.ib()
    infiles = attr.ib()


@attr.s(frozen=True)
class Result:
    jobid = attr.ib()
    state = attr.ib()
    cwd = attr.ib()


types = {'int': int, 'float': float, 'str': str}


class SpecError(Exception):
    pass


def gen_launch_specs(site_spec, prog_spec, paramdicts):
    params = {}
    for k, v in site_spec.params.items():
        params[k] = types[v]
    for k, v in prog_spec.params.items():
        if k in params:
            raise SpecError(
                'some vars present in both site and program specs')
        params[k] = types[v]

    for index, paramdict in enumerate(paramdicts):
        for k, v in paramdict.items():
            if params.get(k) is not None and not isinstance(v, params[k]):
                raise SpecError(f'{k}: expected {params[k]}, got {type(v)}')

        args = [Template(arg).substitute(paramdict) for arg in prog_spec.args]

        ifcs = []
        for infilespec in prog_spec.in_file_specs:
            ifcs.append(
                InFileCreationSpec(
                    infilespec.name,
                    Template(infilespec.template).substitute(paramdict)))

        yield LaunchSpec(args, paramdict, ifcs)


def create_experiment_inputs(machine, launch_profile, launch_specs):
    exp_dir = machine.mkdtemp(launch_profile.base_dir + '/experiments')
    _logger.info(f'Experiment dir is {exp_dir}')

    outdir = launch_profile.base_dir + '/out'
    machine.mkdir(outdir)

    file_templates = []
    cwds = []

    for index, launch_spec in enumerate(launch_specs):
        _logger.info(f'Creating input dir {index+1} of {len(launch_specs)}')
        rundir = f'{exp_dir}/{index}'
        machine.mkdir(rundir)
        cwds.append(rundir)

        for ifcs in launch_spec.infiles:
            fname, contents = ifcs.name, ifcs.contents
            fname = f'{rundir}/{fname}'
            machine.put_file(fname, contents)

    return cwds, outdir


def schedule_jobs(paramdicts, throttles):
    '''
    Generates batches of jobs such that for each (key, value) pair in throttles, the sum
    of values of the parameter `key` in jobspecs never exceeds `value` at any given point
    in time. The jobs that have completed are given to the generator using send().
    '''
    for p in paramdicts:
        if any(throttles.get(k) is not None and p[k] > throttles[k]
               for k in p):
            raise SpecError('some parameters exceed their throttle values')

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

        self._evt = threading.Event()
        self._alive_evt = threading.Event()
        self._done = False

        self._schedobj = sched.scheduler()

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
            if status not in final_states:
                return

            if status == JobStateType.FAIL_EXTERNAL:
                self._attempt_counts[index] += 1
                if self._attempt_counts[index] < self._max_attempts_fail_external:
                    _logger.warning(f'Will resubmit {jobid}')
                    self._batch.append(index)
                else:
                    _logger.error(f'{jobid} failed {self._max_attempts_fail_external} times, will not resubmit')
                return

            _logger.info(f'{jobid} aka {index} done')
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


def get_file_from_glob(machine, dirname, glob_pattern):
    it = machine.list_files(dirname)
    for filename in it:
        if fnmatch.fnmatch(filename, glob_pattern):
            return machine.get_file(f'{dirname}/{filename}')

    raise FileNotFoundError


# TODO: rewrite
def parse_outputs(machine, launch_profile, prog_spec, launch_specs,
                  results):
    def update_result(result, machine, dirname, fname, spec, exact_file=False):
        try:
            _logger.debug(f'Trying to get {dirname}/{fname}')
            if exact_file:
                data = machine.get_file(f'{dirname}/{fname}')
            else:
                data = get_file_from_glob(machine, dirname, fname)
        except OSError:
            _logger.error(f'{dirname}/{fname}: file load failed')
        else:
            if spec:
                _logger.debug(f'Parsing {dirname}/{fname} with spec {spec}')
            for outspec in spec:
                _logger.debug(str(spec))
                vartypes, regex = outspec.vartypes, outspec.regex
                grok = Grok(regex)
                sre_match = grok.search(data)

                for key, value in sre_match.groupdict().items():
                    if key not in vartypes:
                        raise SpecError(f'{key} not in vartypes')

                    typ = types[vartypes[key]]
                    value = typ(value)

                    result[key] = value

    all_output_keys = set()

    for outfilespec in launch_profile.out_file_specs:
        for outspec in outfilespec.outputspecs:
            all_output_keys.update(outspec.vartypes.keys())
    for outfilespec in prog_spec.out_file_specs:
        for outspec in outfilespec.outputspecs:
            all_output_keys.update(outspec.vartypes.keys())
    for outspec in prog_spec.stdout:
        all_output_keys.update(outspec.vartypes.keys())

    _logger.debug(all_output_keys)
    ret = []

    for index, lspec in enumerate(launch_specs):
        paramdict = lspec.params
        paramkeys = paramdict.keys()

        if not paramdict.keys().isdisjoint(all_output_keys):
            raise SpecError('some output vars are also input vars')

        result = paramdict.copy()
        for k in all_output_keys:
            result[k] = None

        no_result = False
        try:
            job_result = results[index]
        except KeyError:
            no_result = True
        else:
            if job_result.state.status != JobStateType.COMPLETED:
                no_result = True

        if no_result:
            ret.append(result)
            continue

        rundir = job_result.cwd

        for spec in prog_spec.out_file_specs:
            update_result(result, machine, rundir, spec.name,
                          spec.outputspecs)
        for spec in launch_profile.out_file_specs:
            update_result(result, machine, rundir, spec.name,
                          spec.outputspecs)
        update_result(
            result, machine,
            f'{launch_profile.base_dir}/out', f'{job_result.jobid}.out',
            prog_spec.stdout, True)

        ret.append(result)

    return list(paramkeys) + list(all_output_keys), ret
