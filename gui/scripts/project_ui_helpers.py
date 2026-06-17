"""Pure, UI-agnostic helpers for the project panel.

Kept free of Solara/ipyvuetify so the display and validation logic can be
unit-tested without a render harness.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

_NAME_RE = re.compile(r"[A-Za-z0-9 _-]+")


def compute_app_title(project, dirty: bool, base: str = "Spatial Risk") -> str:
    """Header title: '<base>' with no project, else '<base> — <name>' plus a
    '●' when there are unsaved changes. Plain text (header cannot host a widget)."""
    if project is None:
        return base
    suffix = " ●" if dirty else ""
    return f"{base} — {project.project_name}{suffix}"


def format_relative(when: datetime, now: datetime) -> str:
    """Coarse relative time: 'just now' / 'N min ago' / 'N hr ago' / 'N days ago'."""
    seconds = max(0, int((now - when).total_seconds()))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60} min ago"
    if seconds < 86400:
        return f"{seconds // 3600} hr ago"
    return f"{seconds // 86400} days ago"


def format_last_saved(when: Optional[datetime], now: datetime) -> str:
    """'never saved' when None, else 'saved <relative>'."""
    if when is None:
        return "never saved"
    return f"saved {format_relative(when, now)}"


@dataclass
class NameValidation:
    valid: bool
    cleaned: str
    exists: bool
    error: Optional[str] = None


def validate_project_name(name: str, existing_names: list[str]) -> NameValidation:
    """Validate a new project name. Non-empty and filesystem-safe
    ([A-Za-z0-9 _-]). ``exists`` is informational (a warning, not an error)."""
    cleaned = name.strip()
    if not cleaned:
        return NameValidation(False, "", False, "Name cannot be empty")
    if not _NAME_RE.fullmatch(cleaned):
        return NameValidation(
            False,
            cleaned,
            False,
            "Use only letters, numbers, spaces, hyphens and underscores",
        )
    return NameValidation(True, cleaned, cleaned in existing_names, None)


def overwrite_needed(
    name: str, last_saved: Optional[datetime], existing_names: list[str]
) -> bool:
    """True only when this is a new/never-saved-this-session project whose name
    already exists on disk — i.e. saving would clobber a different project."""
    return last_saved is None and name in existing_names
