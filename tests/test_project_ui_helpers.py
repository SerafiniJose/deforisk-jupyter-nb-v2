from datetime import datetime

from gui.scripts.project_ui_helpers import (
    NameValidation,
    compute_app_title,
    format_last_saved,
    format_relative,
    overwrite_needed,
    validate_project_name,
)


class _P:
    def __init__(self, name):
        self.project_name = name


def test_app_title_no_project():
    assert compute_app_title(None, False) == "Spatial Risk"


def test_app_title_clean():
    assert compute_app_title(_P("mtq"), False) == "Spatial Risk — mtq"


def test_app_title_dirty():
    assert compute_app_title(_P("mtq"), True) == "Spatial Risk — mtq ●"


def test_format_relative_buckets():
    now = datetime(2026, 6, 17, 12, 0, 0)
    assert format_relative(datetime(2026, 6, 17, 11, 59, 30), now) == "just now"
    assert format_relative(datetime(2026, 6, 17, 11, 55, 0), now) == "5 min ago"
    assert format_relative(datetime(2026, 6, 17, 9, 0, 0), now) == "3 hr ago"
    assert format_relative(datetime(2026, 6, 15, 12, 0, 0), now) == "2 days ago"


def test_format_last_saved_never():
    assert format_last_saved(None, datetime(2026, 6, 17, 12, 0, 0)) == "never saved"


def test_format_last_saved_relative():
    now = datetime(2026, 6, 17, 12, 5, 0)
    when = datetime(2026, 6, 17, 12, 0, 0)
    assert format_last_saved(when, now) == "saved 5 min ago"


def test_validate_empty():
    v = validate_project_name("   ", [])
    assert isinstance(v, NameValidation)
    assert v.valid is False
    assert "empty" in v.error.lower()


def test_validate_bad_chars():
    v = validate_project_name("a/b", [])
    assert v.valid is False
    assert v.error


def test_validate_ok_unique():
    v = validate_project_name("  my-proj ", ["other"])
    assert v.valid is True
    assert v.cleaned == "my-proj"
    assert v.exists is False
    assert v.error is None


def test_validate_ok_but_exists():
    v = validate_project_name("mtq", ["mtq", "other"])
    assert v.valid is True
    assert v.exists is True


def test_overwrite_needed():
    assert overwrite_needed("mtq", None, ["mtq"]) is True
    assert overwrite_needed("mtq", datetime(2026, 1, 1), ["mtq"]) is False
    assert overwrite_needed("fresh", None, ["mtq"]) is False
