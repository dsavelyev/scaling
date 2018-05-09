import scaling.launch as sl
import pytest
from functools import reduce


@pytest.mark.parametrize('jobspecs,throttles', [
    (
        [
            {'x': 4, 'y': 2, 'z': 3},
            {'x': 3, 'y': 1, 'z': 2},
            {'x': 1, 'y': 0, 'z': 1},
            {'x': 2, 'y': 42, 'z': 100500},
            {'x': 3, 'y': 42, 'z': 100500}
        ],
        {'x': 5}
    ),
])
def test_schedule_jobs(jobspecs, throttles):
    generator = sl.schedule_jobs(jobspecs, throttles)

    in_flight_jobs = set()
    done_jobs = set()
    completed_job = None

    while True:
        try:
            new_batch = generator.send(completed_job)
            assert all(0 <= x < len(jobspecs) for x in new_batch)
            assert set(new_batch).isdisjoint(in_flight_jobs | done_jobs)
            in_flight_jobs.update(new_batch)
        except StopIteration:
            assert in_flight_jobs | done_jobs == set(range(len(jobspecs)))
            break
        else:
            assert all(sum(jobspecs[x][name] for x in in_flight_jobs) <= value
                       for name, value in throttles.items())

            completed_job = min(in_flight_jobs)  # eh, good enough
            in_flight_jobs.remove(completed_job)
            done_jobs.add(completed_job)


@pytest.mark.parametrize('jobspecs', [
    [
        {'x': 4, 'y': 2, 'z': 3},
        {'x': 3, 'y': 1, 'z': 2},
        {'x': 1, 'y': 0, 'z': 1},
    ],
])
def test_schedule_jobs_1pass(jobspecs):
    generator = sl.schedule_jobs(jobspecs, {})

    batch = next(generator)
    assert set(batch) == set(range(len(jobspecs)))

    with pytest.raises(StopIteration):
        next(generator)
