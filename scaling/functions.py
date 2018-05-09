# TODO: error reporting infrastructure
# TODO: stop depending on sympy

import itertools
import math

import sympy
import sympy.utilities.enumerative as sue
import sympy.utilities.iterables as sui
import toolz
from mpmath.libmp import isqrt


class FunctionError:
    pass


def ilog_floor(x, b):
    if not isinstance(x, int) or not isinstance(b, int):
        raise FunctionError('ilog_floor: unsupported argument type')
    if x < 1 or b < 1:
        raise FunctionError('ilog_floor: x < 1 or b < 1')

    ret = -1
    while x >= 1:
        x //= b
        ret += 1
    return ret


def ilog_ceil(x, b):
    if not isinstance(x, int) or not isinstance(b, int):
        raise FunctionError('ilog_ceil: unsupported argument type')
    if x < 1 or b < 1:
        raise FunctionError('ilog_ceil: x < 1 or b < 1')
    elif x == 1:
        return 0
    else:
        return ilog_floor(x - 1, b) + 1


def multipartitions(x, count, incl_ones):
    if not isinstance(x, int) or not isinstance(count, int)\
        or not isinstance(incl_ones, int):
        raise FunctionError('multipartitions: unsupported argument type')
    if x <= 0 or count <= 0:
        raise FunctionError('multipartitions: x and count must be positive')

    primes, multiplicities = zip(*sympy.factorint(x).items())
    mtp = sue.MultisetPartitionTraverser()

    if incl_ones:
        it = mtp.enum_small(multiplicities, count)
    else:
        it = mtp.enum_range(multiplicities, count - 1, count)

    it = map(lambda x: sue.factoring_visitor(x, primes), it)
    if incl_ones:
        it = map(lambda x: tuple(list(x) + [1] * (count - len(x))), it)

    it = toolz.concat(map(sui.multiset_permutations, it))

    return it


def isqrt_floor(x):
    if not isinstance(x, int):
        raise FunctionError('isqrt: unsupported argument type')
    if x < 0:
        raise FunctionError('isqrt: x < 0')
    return isqrt(x)


def isqrt_ceil(x):
    if not isinstance(x, int):
        raise FunctionError('isqrt: unsupported argument type')
    if x < 0:
        raise FunctionError('isqrt: x < 0')
    elif x == 0:
        return 0
    else:
        return isqrt_floor(x - 1) + 1


def our_range(*args):
    if len(args) == 1:
        start, stop, step = 1, args[0], 1
    elif 2 <= len(args) <= 3:
        start, stop, step = args[0], args[1], args[2] if len(args) == 3 else 1
    else:
        raise FunctionError('range: wrong number of arguments')

    if any(not isinstance(x, int) for x in (start, stop, step)):
        raise FunctionError('range: arguments must be ints')

    return range(start, stop + 1, step)


def reraise_exc(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise FunctionError(str(e))

    return inner


builtin_funcs = {
    'range': our_range,
    'multipartitions': multipartitions,
    'isqrt_floor': isqrt_floor,
    'isqrt_ceil': isqrt_ceil,
    'sqrt': reraise_exc(lambda x: math.sqrt(x)),
    'log': reraise_exc(lambda x, base: math.log(x, base)),
    'ilog_floor': ilog_floor,
    'ilog_ceil': ilog_ceil,
    'floor': reraise_exc(lambda x: int(math.floor(x))),
    'ceil': reraise_exc(lambda x: int(math.ceil(x))),
    'round': reraise_exc(lambda x: int(round(x))),
    'int': reraise_exc(lambda x: int(x)),
    'float': reraise_exc(lambda x: float(x)),
    'str': reraise_exc(lambda x: str(x)),
    'zip': reraise_exc(lambda *args: zip(*args)),
    'concat': reraise_exc(lambda *args: itertools.chain(*args)),
}
