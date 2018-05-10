import scaling.jobs as sj
import scaling.launch as sl
import pytest
from functools import reduce


@pytest.mark.parametrize('jobspecs,throttles,failures,max_attempts', [
    (
        [
            {'x': 4, 'y': 2, 'z': 3},
            {'x': 3, 'y': 1, 'z': 2},
            {'x': 1, 'y': 0, 'z': 1},
            {'x': 2, 'y': 42, 'z': 100500},
            {'x': 3, 'y': 42, 'z': 100500},
        ],
        {'x': 5},
        {2: 2, 3: 4},
        3
    ),
    (
        [], {}, {}, 1
    ),
])
def test_schedule_jobs_new(jobspecs, throttles, failures, max_attempts):
    generator = sl.schedule_jobs(jobspecs, throttles, max_attempts)

    in_flight_jobs = set()
    done_jobs = set()
    completed_job = None
    failure = False
    num_failures = 0

    while True:
        try:
            new_batch = generator.send(None if completed_job is None else (
                completed_job, sj.JobState(
                    sj.JobStateType.COMPLETED if not failure
                    else sj.JobStateType.FAIL_EXTERNAL, 0)))
        except StopIteration:
            break

        if failure and num_failures < max_attempts:
            done_jobs.remove(completed_job)
            assert new_batch == [completed_job]

        assert all(0 <= x < len(jobspecs) for x in new_batch)
        assert set(new_batch).isdisjoint(in_flight_jobs | done_jobs)

        in_flight_jobs.update(new_batch)

        assert all(sum(jobspecs[x][name] for x in in_flight_jobs) <= value
                   for name, value in throttles.items())

        completed_job = min(in_flight_jobs)  # eh, good enough
        if failures.get(completed_job, 0) > 0:
            failures[completed_job] -= 1
            failure = True
            num_failures += 1
        else:
            failure = False
            num_failures = 0
        in_flight_jobs.remove(completed_job)
        done_jobs.add(completed_job)

    assert done_jobs == set(range(len(jobspecs)))
    assert not in_flight_jobs


@pytest.mark.parametrize('jobspecs', [
    [
        {'x': 4, 'y': 2, 'z': 3},
        {'x': 3, 'y': 1, 'z': 2},
        {'x': 1, 'y': 0, 'z': 1},
    ],
])
def test_schedule_jobs_new_1pass(jobspecs):
    generator = sl.schedule_jobs(jobspecs, {}, 1)

    batch = generator.send(None)
    assert set(batch) == set(range(len(jobspecs)))
