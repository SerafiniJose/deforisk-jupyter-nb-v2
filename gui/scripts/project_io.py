"""Project load/save helpers for the Spatial Risk GUI."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from spatialrisk.project import Project


def load_project(project_name: str) -> Project:
    """Load a project from disk by name."""
    return Project.load(project_name)


def save_project(project: Project) -> Path:
    """Save a project to disk, returns the saved path."""
    return project.save()


def list_projects(data_dir: Path) -> list[str]:
    """Return names of all saved projects found under data_dir."""
    return sorted(
        p.name for p in data_dir.iterdir()
        if p.is_dir() and (p / f"{p.name}_project.json").exists()
    )


@dataclass
class ProjectInfo:
    """Lightweight metadata for one saved project, for the load picker."""

    name: str
    raw_count: int
    processed_count: int
    model_count: int
    modified: Optional[datetime]
    readable: bool
    error: Optional[str] = None


def list_project_infos(data_dir: Path) -> list[ProjectInfo]:
    """Scan ``data_dir`` and return metadata for each saved project.

    A directory counts as a project only if it contains
    ``{name}_project.json``. The JSON is parsed for variable/model counts and
    the file mtime; a corrupt/unreadable file yields ``readable=False`` with an
    ``error`` reason rather than being dropped silently.
    """
    infos: list[ProjectInfo] = []
    if not data_dir.exists():
        return infos
    for child in sorted(data_dir.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        json_path = child / f"{child.name}_project.json"
        if not json_path.exists():
            continue
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            modified = datetime.fromtimestamp(json_path.stat().st_mtime)
            infos.append(
                ProjectInfo(
                    name=child.name,
                    raw_count=len(data.get("raw_variables", {})),
                    processed_count=len(data.get("processed_variables", {})),
                    model_count=len(data.get("models", {})),
                    modified=modified,
                    readable=True,
                )
            )
        except Exception as exc:  # corrupt JSON, permissions, etc.
            infos.append(
                ProjectInfo(
                    name=child.name,
                    raw_count=0,
                    processed_count=0,
                    model_count=0,
                    modified=None,
                    readable=False,
                    error=str(exc),
                )
            )
    return infos
