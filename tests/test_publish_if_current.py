"""A finished background job must not write its project back over a newer one.

Jobs capture a Project when they start. If the user deleted it (project → None)
or switched to another project meanwhile, publishing the captured reference puts
a dead project back into app state — and its auto-save re-creates the folder that
was just deleted.
"""

import solara

from gui.scripts.solara_threads import publish_if_current


class _P:
    """Minimal stand-in for a Project (model_copy is all the publisher needs)."""

    def __init__(self, name):
        self.project_name = name

    def model_copy(self):
        return _P(self.project_name)


def test_publishes_when_the_project_is_still_the_open_one():
    p = _P("GUY")
    reactive = solara.reactive(p, equals=lambda a, b: a is b)

    assert publish_if_current(reactive, p) is True
    # A fresh copy is published, so identity-equality reactives actually fire.
    assert reactive.value is not p
    assert reactive.value.project_name == "GUY"


def test_refuses_when_the_project_was_deleted():
    p = _P("GUY")
    reactive = solara.reactive(None, equals=lambda a, b: a is b)

    assert publish_if_current(reactive, p) is False
    assert reactive.value is None  # stays closed — no resurrection


def test_refuses_when_the_user_switched_projects():
    finished_job_project = _P("GUY")
    reactive = solara.reactive(_P("MTQ"), equals=lambda a, b: a is b)

    assert publish_if_current(reactive, finished_job_project) is False
    assert reactive.value.project_name == "MTQ"  # not clobbered by the old job


def test_publishes_after_another_tile_copied_the_same_project():
    # Tiles routinely republish via project.set(p.model_copy()), so the live
    # object is a *different instance* of the same project. A long job finishing
    # afterwards must still be able to show its result — an identity check would
    # silently drop it.
    p = _P("GUY")
    reactive = solara.reactive(p.model_copy(), equals=lambda a, b: a is b)

    assert publish_if_current(reactive, p) is True
    assert reactive.value.project_name == "GUY"


def test_tolerates_none_inputs():
    assert publish_if_current(None, _P("GUY")) is False
    assert publish_if_current(solara.reactive(None), None) is False
