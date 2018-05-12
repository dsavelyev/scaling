from concurrent.futures import ThreadPoolExecutor
import fnmatch
import logging
import os.path
import queue
from string import Template
import threading
import time

import attr
from .jobs import JobSpec, JobState, JobStateType, SchedulerError, final_states
from .grok import Grok
from .machine import SSHMachine
from .util import call_later


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


def schedule_jobs(paramdicts, throttles, max_attempt_counts=10):
    throttle_vars = {}
    for name, value in throttles.items():
        throttle_vars[name] = 0

    attempt_counts = [0] * len(paramdicts)

    iterator = enumerate(paramdicts)
    try:
        curjob = next(iterator)
    except StopIteration:
        curjob = None

    num_pending = len(paramdicts)
    batch = []

    while num_pending:
        while curjob is not None:
            index, paramdict = curjob
            if all(paramdict[name] + throttle_vars[name] <= value
                   for name, value in throttles.items()):
                batch.append(index)
                attempt_counts[index] = 1
                for name in throttle_vars:
                    throttle_vars[name] += paramdict[name]
            else:
                break

            try:
                curjob = next(iterator)
            except StopIteration:
                curjob = None

        while True:
            index, state = yield batch

            if state.status == JobStateType.FAIL_EXTERNAL:
                attempt_counts[index] += 1
                if attempt_counts[index] <= max_attempt_counts:
                    batch = [index]
                else:
                    break
            elif state.status not in final_states:
                batch = []
            else:
                break

        findict = paramdicts[index]
        for name in throttle_vars:
            throttle_vars[name] -= findict[name]

        num_pending -= 1
        batch = []


class ThrottlingSubmitter(threading.Thread):
    def __init__(self,
                 scheduler,
                 jobspecs,
                 strategy,
                 attempt_interval=15):
        super().__init__()

        self._scheduler = scheduler
        self._jobspecs = jobspecs
        self._strategy = strategy
        self._attempt_interval = attempt_interval

        self._q = queue.Queue()
        self._alive_evt = threading.Event()
        self._done = False
        self._do_submit = True

        self._timer_tpe = ThreadPoolExecutor(max_workers=1)
        self._timer_cancel_evt = threading.Event()

        self._batch = queue.deque()
        self._jobids = {}
        self._jobs = {}
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

        self._batch.extend(self._strategy.send(None))
        _logger.debug(f'Received batch {self._batch}')

        while not self._done:
            if self._do_submit and self._batch and self._submit_one_job():
                block = not self._batch
            else:
                block = True

            try:
                evt = self._q.get(block=block)
            except queue.Empty:
                continue
            else:
                self._handle_event(evt)

        _logger.debug('Exited from the event loop')

        self._timer_cancel_evt.set()
        self._timer_tpe.shutdown()

        self._results = {}
        for jobindex, job in self._jobs.items():
            self._results[jobindex] = (job.jobid, job.state)

    def _listener(self, jobid, state):
        self._fire_event((jobid, state))

    def _fire_event(self, event):
        _logger.debug(f'Firing event {event}')
        self._q.put(event)

    def _handle_event(self, event):
        _logger.debug(f'Handling event {event}')

        if event == 'STOP':
            _logger.debug('Stopping')
            self._done = True
        elif event == 'SUBMIT':
            self._do_submit = True
        else:
            jobid, state = event

            index = self._jobids[jobid]
            _logger.info(f'Job {index} (id {jobid}) entered state {state}')

            try:
                newjobs = self._strategy.send((index, state))
            except StopIteration:
                _logger.info('All jobs done')
                self._done = True
            else:
                _logger.debug(f'Received batch {newjobs}')
                if newjobs:
                    self._batch.extend(newjobs)

    def _submit_one_job(self):
        index = self._batch[0]
        spec = self._jobspecs[index]

        _logger.info(
            f'Attempting to submit job {index}')

        try:
            job = self._scheduler.submit_job(spec, self._listener)
        except SchedulerError as e:
            _logger.error(f'Submit error (will retry): {e}')
            self._do_submit = False
            self._timer_tpe.submit(call_later,
                self._attempt_interval, self._timer_cancel_evt, self._fire_event,
                'SUBMIT')
            return False

        _logger.info(f'Job {index} submitted with id {job.jobid}')

        self._batch.popleft()
        self._jobids[job.jobid] = index
        self._jobs[index] = job
        return True


def get_file_from_glob(machine, dirname, glob_pattern):
    it = machine.list_files(dirname)
    for filename in it:
        if fnmatch.fnmatch(filename, glob_pattern):
            return machine.get_file(f'{dirname}/{filename}')

    raise FileNotFoundError


# TODO: rewrite
def parse_outputs(machine, launch_profile, prog_spec, launch_specs,
                  results):
    def update_result(result, dirname, fname, spec, exact_file=False):
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
            update_result(result, rundir, spec.name,
                          spec.outputspecs)
        for spec in launch_profile.out_file_specs:
            update_result(result, rundir, spec.name,
                          spec.outputspecs)
        update_result(
            result,
            f'{launch_profile.base_dir}/out', f'{job_result.jobid}.out',
            prog_spec.stdout, True)

        ret.append(result)

    return list(paramkeys) + list(all_output_keys), ret
