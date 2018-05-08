import scaling.functions as sf
import itertools


def test_ilog():
    for func in (sf.ilog_floor, sf.ilog_ceil):
        assert func(4, 2) == 2
        assert func(8, 2) == 3
        assert func(25, 5) == 2

    assert sf.ilog_floor(38, 2) == 5
    assert sf.ilog_ceil(38, 2) == 6


def test_isqrt():
    for func in (sf.isqrt_floor, sf.isqrt_ceil):
        assert func(4) == 2
        assert func(36) == 6

    assert sf.isqrt_floor(38) == 6
    assert sf.isqrt_ceil(38) == 7


def test_concat():
    assert list(sf.builtin_funcs['concat'](
        [1], [2, 3],
        sf.builtin_funcs['range'](3),
        [42])) == [1, 2, 3, 1, 2, 3, 42]
