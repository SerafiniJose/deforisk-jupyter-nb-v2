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
