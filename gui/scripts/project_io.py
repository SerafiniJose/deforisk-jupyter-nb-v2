"""Project load/save helpers for the Spatial Risk GUI."""

from pathlib import Path

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
