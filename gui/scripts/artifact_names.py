"""Solara-free helpers for artifact naming.

One home for the name conventions every creation form shares: suggested
default names, key sanitizing, and the (message, error) states of the shared
ArtifactNameField. Imported by tiles and form dialogs; never imports solara.
"""

import re
from typing import Optional, Tuple


def suggest_name(prefix: str, taken) -> str:
    """Smallest '<prefix>_<n>' (n>=1) not already in `taken`."""
    n = 1
    while f"{prefix}_{n}" in taken:
        n += 1
    return f"{prefix}_{n}"


def suggest_version(model_key: str, taken_model_keys) -> str:
    """Smallest 'v<n>' whose storage key '<model_key>_v<n>' is free."""
    n = 1
    while f"{model_key}_v{n}" in taken_model_keys:
        n += 1
    return f"v{n}"


def sanitize_key(name: Optional[str]) -> str:
    """Normalise a user-typed name to a key/path-safe token.

    Keeps alphanumerics, dash and underscore; collapses any other run into a
    single underscore and trims leading/trailing ones. Mirrors the historic
    train/inference tile behaviour (they now alias this function).
    """
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (name or "").strip()).strip("_")


def default_pred_name(model_key: str, dataset_name: str) -> str:
    """Prefilled prediction name for a (model, dataset) selection."""
    if not model_key or not dataset_name:
        return ""
    return sanitize_key(f"{model_key}__{dataset_name}")


def prediction_name_exists(project, name: str) -> bool:
    """True if a prediction already uses *name* (as its key or name field)."""
    if project is None or not name:
        return False
    if name in getattr(project, "predictions", {}):
        return True
    return bool(project.filter_predictions(name=name))


def name_field_messages(
    clean_key: str, exists: bool, attempted: bool
) -> Tuple[str, bool]:
    """(i18n message key, is_error) for an ArtifactNameField helper line.

    Empty name is only *flagged* red after a submit attempt; an existing key
    is a warning (creation will ask to confirm), never a blocker.
    """
    if not clean_key:
        return "widgets.artifact_name.required", attempted
    if exists:
        return "widgets.artifact_name.exists_warning", False
    return "widgets.artifact_name.saved_as", False
