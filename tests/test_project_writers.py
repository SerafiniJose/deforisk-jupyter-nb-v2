"""Which project folders have a background task writing into them right now.

Delete consults this: removing a folder while a task is still saving into it
lets that task's auto-save re-create the folder we just deleted.
"""

import pytest

from gui.store.project_writers import is_writing, project_writers, writing


@pytest.fixture(autouse=True)
def _clean():
    project_writers.set({})
    yield
    project_writers.set({})


def test_marks_a_project_while_the_block_runs():
    assert is_writing("GUY") is False
    with writing("GUY"):
        assert is_writing("GUY") is True
    assert is_writing("GUY") is False


def test_is_scoped_to_the_named_project():
    with writing("GUY"):
        assert is_writing("MTQ") is False


def test_counts_overlapping_writers():
    with writing("GUY"):
        with writing("GUY"):
            assert is_writing("GUY") is True
        # One writer finished, the other has not.
        assert is_writing("GUY") is True
    assert is_writing("GUY") is False


def test_clears_the_mark_when_the_body_raises():
    # A crashed job must not block deletes forever.
    with pytest.raises(RuntimeError):
        with writing("GUY"):
            raise RuntimeError("boom")
    assert is_writing("GUY") is False


def test_finished_writers_leave_no_residue():
    with writing("GUY"):
        pass
    assert project_writers.value == {}
