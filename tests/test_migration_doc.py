# tests/test_migration_doc.py
"""MIGRATION.md must document every legacy->Session call-site mapping."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOC = REPO_ROOT / "docs" / "MIGRATION.md"


def test_migration_doc_exists():
    assert DOC.is_file(), "docs/MIGRATION.md is missing"


def test_migration_doc_covers_every_mapping():
    text = DOC.read_text()
    # before/after column markers
    assert "| Before " in text and "| After " in text
    # each legacy -> new mapping from spec §15 must be present
    required = [
        "Project(",
        "ProjectSession.create",
        "ProjectSession.open",
        "GEEVar(",
        "add_gee_variable",
        "CatalogueRecipe",
        "add_as_raw",
        "add_local_raster",
        "reproject_and_match_all",
        "process_all",
        "ModelHandle",
        "session.save()",
    ]
    for token in required:
        assert token in text, f"MIGRATION.md missing mapping for: {token}"
