import argparse
import csv
import functools
import logging
import os
import re
import sys

import attr
import pytoml
from .launch import MachineSpec, LaunchProfile, ProgSpec, OutFileSpec, InFileSpec, LaunchSpec,\
    InFileCreationSpec, OutputSpec, JobState, JobStateType, Result, gen_launch_specs, run_experiment, parse_outputs,\
    types as _param_types, SpecError
from .exprs import gen_params, ParamSpecParser, ParamSpecError
from .machine import SSHMachine


_logger = logging.getLogger(__name__)


class LogFormatter(logging.Formatter):
    def format(self, record):
        return f'[{record.name}] {record.getMessage()}'


class ConfigError(Exception):
    pass


class UserError(Exception):
    pass


def _open_file(fname, src_fname):
    try:
        f = open(fname, 'r')
    except OSError as e:
        raise ConfigError(f'{src_fname}: opening {fname}: {e.strerror}')
    return f


def _toml_from_file(file):
    try:
        ret = pytoml.load(file)
    except pytoml.TomlError as e:
        raise ConfigError(f'{file.name}: TOML is malformed: {e}')
    return ret


def _toml_from_filename(fname, src_fname):
    with _open_file(fname, src_fname) as f:
        return _toml_from_file(f)


def _validate_schema(d, schema, fname):
    for name, (typ, defval) in schema.items():
        val = d.get(name, defval)
        if val is None:
            raise ConfigError(f'{fname}: required parameter {name} not found')
        if not isinstance(val, typ):
            raise ConfigError(f'{fname}: {name}: type mismatch')

        d.setdefault(name, defval)


_outfilespec_schema = {
    'name': (str, None),
    'outputspecs': (list, None)
}


def get_outfilespec(ofs, fname):
    _validate_schema(ofs, _outfilespec_schema, fname)

    outputspecs = list(map(functools.partial(get_outputspec, fname=fname),
                       ofs['outputspecs']))
    return OutFileSpec(ofs['name'], outputspecs)


_outputspec_schema = {
    'vartypes': (dict, None),
    'regex': (str, None),
}


def get_outputspec(spec, fname):
    _validate_schema(spec, _outputspec_schema, fname)
    _validate_param_types(spec['vartypes'], fname)

    return OutputSpec(spec['vartypes'], spec['regex'])


def _validate_param_types(params, fname):
    if not isinstance(params, dict):
        raise ConfigError(f'{fname}: params must be a dictionary')

    for k, v in params.items():
        if not re.match(ParamSpecParser.ident_regex, k):
            raise ConfigError(
                f'{fname}: parameter {k} is not a valid identifier')
        if v not in _param_types:
            raise ConfigError(
                f'{fname}: parameter {k}: {v} is not one of int, float, str')


_machine_spec_schema = {
    'host': (str, None),
    'username': (str, None),
    'port': (int, 22)
}


def load_machine_spec(file):
    d = _toml_from_file(file)
    _validate_schema(d, _machine_spec_schema, file.name)

    return MachineSpec(**d)


_launch_profile_schema = {
    'machine': (str, None),
    'start_cmd': (str, None),
    'poll_cmd': (str, None),
    'cancel_cmd': (str, None),
    'base_dir': (str, None),
    'params': (dict, {}),
    'param_order': (list, []),
    'out_file_specs': (list, [])
}


def load_launch_profile(file):
    d = _toml_from_file(file)
    _validate_schema(d, _launch_profile_schema, file.name)

    if not all(x in d['params'].keys() for x in d['param_order']):
        raise ConfigError(f'{file.name}: unknown parameters in param_order')

    with _open_file(d['machine'], file.name) as f:
        d['machine'] = load_machine_spec(f)

    d['out_file_specs'] = list(map(functools.partial(get_outfilespec, fname=file.name), d['out_file_specs']))

    return LaunchProfile(**d)


_infilespec_schema = {
    'name': (str, None),
    'template': (str, None)
}


def get_infilespec(d, fname):
    _validate_schema(d, _infilespec_schema, fname)

    with _open_file(d['template'], fname) as f:
        template = f.read()

    return InFileSpec(d['name'], template)


_prog_spec_schema = {
    'params': (dict, None),
    'args': (list, None),
    'stdout': (list, []),
    'out_file_specs': (list, []),
    'in_file_specs': (list, [])
}


def load_prog_spec(file):
    d = _toml_from_file(file)
    _validate_schema(d, _prog_spec_schema, file.name)
    _validate_param_types(d['params'], file.name)

    if not all(isinstance(x, str) for x in d['args']):
        raise ConfigError(f'{file.name}: args: type mismatch')

    d['stdout'] = list(map(functools.partial(get_outputspec, fname=file.name), d['stdout']))
    d['out_file_specs'] = list(map(functools.partial(get_outfilespec, fname=file.name), d['out_file_specs']))
    d['in_file_specs'] = list(map(functools.partial(get_infilespec, fname=file.name), d['in_file_specs']))

    return ProgSpec(**d)


def write_launch_specs(file, launch_profile_file, prog_spec_file, executable, launch_specs):
    all_specs = list(map(attr.asdict, launch_specs))
    pytoml.dump({'launch_profile': launch_profile_file,
                 'prog_spec': prog_spec_file,
                 'executable': executable,
                 'specs': list(all_specs)}, file)


_infile_creation_spec_schema = {
    'name': (str, None),
    'contents': (str, None)
}


def get_infile_creation_spec(spec, fname):
    _validate_schema(spec, _infile_creation_spec_schema, fname)
    return InFileCreationSpec(**spec)


_launch_spec_schema = {
    'args': (list, None),
    'params': (dict, None),
    'infiles': (list, None)
}


_launch_spec_file_schema = {
    'launch_profile': (str, None),
    'prog_spec': (str, None),
    'executable': (str, None),
    'specs': (list, None)
}


def load_launch_specs(file):
    obj = _toml_from_file(file)
    _validate_schema(obj, _launch_spec_file_schema, file.name)

    with _open_file(obj['launch_profile'], file.name) as f:
        launch_profile = load_launch_profile(f)
    with _open_file(obj['prog_spec'], file.name) as f:
        prog_spec = load_prog_spec(f)

    spec_list = []

    for d in obj['specs']:
        _validate_schema(d, _launch_spec_schema, file.name)
        ifcs = list(map(functools.partial(get_infile_creation_spec, fname=file.name), d['infiles']))
        spec_list.append(LaunchSpec(d['args'], d['params'], ifcs))

    return launch_profile, prog_spec, obj['executable'], spec_list


def write_results(file, results):
    ret = []
    for index, res in results.items():
        ret.append({
            'index': index,
            'jobid': str(res.jobid),
            'state': {'status': res.state.status.name, 'exit_code': res.state.exit_code},
            'cwd': res.cwd
        })

    pytoml.dump({'result': ret}, file)


_jobstate_schema = {
    'status': (str, None),
    'exit_code': (int, None)
}


def get_job_state(obj, fname):
    _validate_schema(obj, _jobstate_schema, fname)

    status = JobStateType[obj['status']]
    return JobState(status, obj['exit_code'])


_result_schema = {
    'index': (int, None),
    'jobid': (str, None),
    'state': (dict, None),
    'cwd': (str, None)
}


def load_results(file):
    obj = _toml_from_file(file)
    if not isinstance(obj, dict):
        raise ConfigError('invalid result spec')
    try:
        dlist = obj['result']
    except KeyError:
        raise ConfigError('invalid result spec')

    results = {}

    for d in dlist:
        _validate_schema(d, _result_schema, file.name)
        results[d['index']] = Result(d['jobid'],
                                     get_job_state(d['state'], file.name),
                                     d['cwd'])

    return results


class unlink_on_exception:
    def __init__(self, file):
        self._file = file
        self._filepath = os.path.realpath(file.name)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            self._file.close()
            os.unlink(self._filepath)


def genparams(args):
    with unlink_on_exception(args.launch_spec):
        try:
            site_spec = load_launch_profile(args.launch_profile)
            prog_spec = load_prog_spec(args.prog_spec)
        except ConfigError as e:
            raise UserError(str(e))

        try:
            params = gen_params(args.param_spec.read())
        except ParamSpecError as e:
            raise UserError(str(e))

        params = list(params)
        _logger.info(f'Number of runs: {len(params)}')

        try:
            generator = gen_launch_specs(site_spec, prog_spec, params)
        except SpecError as e:
            raise UserError(str(e))

        write_launch_specs(args.launch_spec, args.launch_profile.name,
                args.prog_spec.name, args.executable, generator)


def launch(args):
    with unlink_on_exception(args.result_file):
        _logger.info('Parsing launch specs...')
        launch_profile, _, executable, launch_specs = load_launch_specs(args.launch_spec)
        throttles = args.throttle

        throttledict = {}
        for k, v in throttles:
            throttledict[k] = int(v)

        machine_spec = launch_profile.machine

        results = run_experiment(machine_spec, launch_profile, executable, launch_specs, throttledict)

        if results is not None:
            write_results(args.result_file, results)

    if results is None:
        os.unlink(args.result_file.name)


def getoutputs(args):
    with unlink_on_exception(args.out):
        _logger.info('Parsing launch specs...')
        launch_profile, prog_spec, _, launch_specs = load_launch_specs(args.launch_spec)
        _logger.info('Parsing results...')
        results = load_results(args.result_file)
        machine_spec = launch_profile.machine

        _logger.info('Downloading and parsing outputs...')
        fieldnames, out = parse_outputs(machine_spec, launch_profile, prog_spec, launch_specs, results)

        _logger.info('Writing CSV...')
        dw = csv.DictWriter(args.out, fieldnames)
        dw.writeheader()
        dw.writerows(out)


def main():
    parser = argparse.ArgumentParser(prog='scaling')
    parser.add_argument('-d', '--debug', help='enable debug logging', action='store_true')

    subparsers = parser.add_subparsers()

    parser_genparams = subparsers.add_parser('genparams')
    parser_genparams.add_argument(
        '-l', '--launch-profile', type=argparse.FileType('r'), required=True)
    parser_genparams.add_argument(
        '-p', '--prog-spec', type=argparse.FileType('r'), required=True)
    parser_genparams.add_argument(
        '-s', '--param-spec', type=argparse.FileType('r'), required=True)
    parser_genparams.add_argument(
        '-e', '--executable', type=str, required=True)
    parser_genparams.add_argument(
        '-o', '--launch-spec', type=argparse.FileType('w'), required=True)
    parser_genparams.set_defaults(func=genparams)

    parser_launch = subparsers.add_parser('launch')
    parser_launch.add_argument(
        '-i', '--launch-spec', type=argparse.FileType('r'), required=True)
    parser_launch.add_argument(
        '-t', '--throttle', nargs=2, metavar=('var', 'value'), action='append',
        required=False, default=[])
    parser_launch.add_argument(
        '-o', '--result-file', type=argparse.FileType('w'), required=True)
    parser_launch.set_defaults(func=launch)

    parser_getoutputs = subparsers.add_parser('getoutputs')
    parser_getoutputs.add_argument(
        '-l', '--launch-spec', type=argparse.FileType('r'), required=True)
    parser_getoutputs.add_argument(
        '-r', '--result-file', type=argparse.FileType('r'), required=True)
    parser_getoutputs.add_argument(
        '-o', '--out', type=argparse.FileType('w'), required=True)
    parser_getoutputs.set_defaults(func=getoutputs)

    args = parser.parse_args()

    handler = logging.StreamHandler()
    handler.setFormatter(LogFormatter())

    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel, handlers=(handler,))

    try:
        args.func(args)
    except UserError as e:
        _logger.exception(str(e))
        sys.exit(1)

if __name__ == '__main__':
    main()