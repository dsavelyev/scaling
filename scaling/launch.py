import fnmatch
import logging
import os.path
from string import Template

import attr
from .jobs import JobSpec, JobState, JobStateType, RemoteScheduler, ThrottlingSubmitter, SchedulerError
from .grok import Grok
from .machine import SSHMachine


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


def create_inputs(machine, exp_dir, launch_specs):
    file_templates = []
    cwds = []

    for index, launch_spec in enumerate(launch_specs):
        _logger.info(f'Creating input dir {index} of {len(launch_specs)}')
        rundir = f'{exp_dir}/{index}'
        machine.mkdir(rundir)
        cwds.append(rundir)

        for ifcs in launch_spec.infiles:
            fname, contents = ifcs.name, ifcs.contents
            fname = f'{rundir}/{fname}'
            machine.put_file(fname, contents)

    return cwds


def create_exp_dir(machine, base_dir):
    return machine.mkdtemp(base_dir + '/experiments')


def run_experiment(machine, launch_profile, executable, launch_specs, throttles):
    for spec in launch_specs:
        if any(throttles.get(k) is not None and spec.params[k] > throttles[k]
               for k in spec.params):
            raise SpecError('some parameters exceed their throttle values')

    with SSHMachine(
            host=machine.host, port=machine.port,
            username=machine.username) as machine:

        exp_dir = create_exp_dir(machine, launch_profile.base_dir)
        _logger.info(f'Experiment dir is {exp_dir}')
        cwds = create_inputs(machine, exp_dir, launch_specs)
        outdir = launch_profile.base_dir + '/out'
        machine.mkdir(outdir)

        scheduler = RemoteScheduler(
            machine,
            launch_profile.start_cmd,
            launch_profile.poll_cmd,
            launch_profile.cancel_cmd,
            launch_profile.param_order,
            outdir,
            poll_interval=15)

        jobspecs = []
        paramdicts = []
        for i, spec in enumerate(launch_specs):
            args = [executable]
            args.extend(spec.args)
            cwd = cwds[i]

            jobspecs.append(JobSpec(args, spec.params, cwd))

            paramdicts.append(spec.params)

        submitter = None
        jobids = None
        results = None
        try:
            submitter = ThrottlingSubmitter(scheduler, jobspecs, paramdicts,
                                            throttles)

            results = submitter.result()

        except KeyboardInterrupt:
            if submitter is not None:
                _logger.info('Stopping')
                submitter.stop()
                results = submitter.result()

                try:
                    _logger.info('Trying to cancel all submitted jobs')
                    scheduler.cancel_jobs(jobids)
                except SchedulerError:
                    _logger.info('Cancel failed')
                    pass
                else:
                    _logger.info('Cancel succeeded')

        ret = {}
        for index, (jobid, state) in results.items():
            ret[index] = Result(jobid, state, cwds[index])

        return ret


def get_file_from_glob(machine, dirname, glob_pattern):
    it = machine.list_files(dirname)
    for filename in it:
        if fnmatch.fnmatch(filename, glob_pattern):
            return machine.get_file(f'{dirname}/{filename}')

    raise FileNotFoundError


# TODO: rewrite
def parse_outputs(machine_spec, launch_profile, prog_spec, launch_specs,
                  results):
    def update_result(result, machine, dirname, fname, spec):
        try:
            _logger.debug(f'Trying to get {dirname}/{fname}')
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

    with SSHMachine(
            host=machine_spec.host,
            port=machine_spec.port,
            username=machine_spec.username) as machine:

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
                prog_spec.stdout)

            ret.append(result)

        return list(paramkeys) + list(all_output_keys), ret
