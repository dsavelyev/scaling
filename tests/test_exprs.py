import pytest

import scaling.exprs as se


def check_names_and_sort_dicts(dicts, names):
    list_of_dicts = []

    for d in dicts:
        if d.keys() != set(names):
            raise ValueError(f'wrong set of names: expected {names}, got {d.keys()}')
        list_of_dicts.append(tuple(map(d.__getitem__, names)))

    return sorted(list_of_dicts)


def test_parser1():
    spec1 = '''
    x1, x2: multipartitions(x, 2, 1);
    x: range(5, 6);
    '''
    names = ('x', 'x1', 'x2')
    res1 = sorted([
        (5, 5, 1),
        (5, 1, 5),
        (6, 1, 6),
        (6, 2, 3),
        (6, 6, 1),
        (6, 3, 2)
    ])

    assert check_names_and_sort_dicts(se.gen_params(spec1), names) == res1


def test_parser2():
    spec2 = '''
    x: 2 + 2 * y;
    y: 2;
    z: ilog_floor(8, t);
    t: y;
    '''
    names = ('x', 'y', 'z', 't')
    res2 = [(6, 2, 3, 2)]

    assert check_names_and_sort_dicts(se.gen_params(spec2), names) == res2

    params2 = se.gen_params(spec2)
    for k, v in next(params2).items():
        assert isinstance(v, int)
    with pytest.raises(StopIteration):
        next(params2)


def test_parser3():
    spec3 = '''
    x: float(4);
    y: 3.0;
    z: int(x);
    t: y + z;
    w: z / z;
    '''
    params3 = se.gen_params(spec3)

    first = next(params3)
    assert isinstance(first['x'], float)
    assert isinstance(first['y'], float)
    assert isinstance(first['z'], int)
    assert isinstance(first['t'], float)
    assert isinstance(first['w'], int)

    with pytest.raises(StopIteration):
        next(params3)


def test_parser4():
    spec4 = '''
    x: [1, 2];
    y: ["abc", "def"];
    z: [x, x, x];
    t: 42;
    t2: [42];
    '''

    names = ('x', 'y', 'z', 't', 't2')
    res4 = sorted([
        (1, "abc", 1, 42, 42),
        (1, "def", 1, 42, 42),
        (2, "abc", 2, 42, 42),
        (2, "def", 2, 42, 42)
    ] * 3)

    assert check_names_and_sort_dicts(se.gen_params(spec4), names) == res4


def test_parser5():
    spec5 = '''
    x, y, z: zip(range(0, 2), range(3, 5), range(6, 8))
    '''

    names = ('x', 'y', 'z')
    res5 = sorted([
        (0, 3, 6),
        (1, 4, 7),
        (2, 5, 8)
    ])

    assert check_names_and_sort_dicts(se.gen_params(spec5), names) == res5


def test_parser6():
    spec6 = '''
    x: [];
    y: range(42);
    z: []
    '''

    params6 = se.gen_params(spec6)
    with pytest.raises(StopIteration):
        next(params6)

def test_parser7():
    spec7 = r'''
    x: "\"";
    y: "\\";
    z: "\\\"\\a";
    '''

    strings = next(se.gen_params(spec7))
    res = {'x': '"', 'y': '\\', 'z': r'\"\a'}
    assert strings == res
