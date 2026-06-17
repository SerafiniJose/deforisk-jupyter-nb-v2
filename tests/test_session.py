from spatialrisk.document import ProjectDocument
from spatialrisk.session import ProjectSession


def _doc(name="proj_d"):
    return ProjectDocument(project_name=name)


def test_from_document_exposes_snapshot_and_version():
    doc = _doc()
    session = ProjectSession.from_document(doc)

    # snapshot() returns the exact inert document back
    assert isinstance(session.snapshot(), ProjectDocument)
    assert session.snapshot().project_name == "proj_d"
    # a fresh session starts at doc_version 0
    assert session.doc_version == 0
    # convenience name accessor mirrors the document
    assert session.project_name == "proj_d"


def test_replace_creates_new_doc_bumps_version_and_freezes_prior_snapshot():
    session = ProjectSession.from_document(_doc("orig"))
    before = session.snapshot()
    before_version = session.doc_version

    returned = session._replace(project_name="renamed")

    # mutation produced a NEW document object, not in-place edit
    assert session.snapshot() is not before
    assert returned is session.snapshot()
    assert session.snapshot().project_name == "renamed"
    # the prior snapshot is unchanged (frozen, references-only)
    assert before.project_name == "orig"
    # version advanced exactly once
    assert session.doc_version == before_version + 1


def test_session_never_uses_model_copy_update_for_doc_state():
    # Regression guard (spec §13): Document state must go through validated
    # _replace, never model_copy(update=...), which skips validation.
    import inspect
    import spatialrisk.session as session_mod

    src = inspect.getsource(session_mod)
    assert "model_copy(update" not in src
    assert ".model_copy(" not in src


import pytest
from pydantic import ValidationError


def test_replace_rejects_non_json_nested_value():
    session = ProjectSession.from_document(_doc())
    before_version = session.doc_version

    class _NotJson:
        pass

    # a non-JSON object smuggled into the AOI GeoJSON map must be rejected
    # by re-validation (GeoJSONGeometry == dict[str, JsonValue]).
    with pytest.raises(ValidationError):
        session._replace(aoi={"bad": _NotJson()})

    # failed mutation leaves the document and version untouched
    assert session.snapshot().aoi is None
    assert session.doc_version == before_version
