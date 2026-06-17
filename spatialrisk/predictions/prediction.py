"""First-class representation of a model prediction output (one raster file)."""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


def build_dataset_snapshot(dataset: Any) -> Dict[str, Any]:
    """Build a compact, serializable snapshot of a dataset's identifying config.

    Deliberately does NOT call ``dataset.model_dump()`` because ``Dataset.project``
    is a non-excluded live reference that would recurse into the entire project.
    """
    if dataset is None:
        return {}
    target = getattr(dataset, "target", None)
    return {
        "name": getattr(dataset, "name", None),
        "year": getattr(dataset, "year", None),
        "target_name": getattr(target, "name", None) if target is not None else None,
        "target_year": getattr(target, "year", None) if target is not None else None,
        "feature_names": [f.name for f in getattr(dataset, "features", [])],
    }


class Prediction(BaseModel):
    """A single model output raster, with frozen model + dataset provenance."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Optional[str] = None
    path: Path
    model_key: str
    dataset_name: str
    year: Optional[int] = None
    window: Optional[int] = None
    active: bool = True
    tags: List[str] = Field(default_factory=list)
    created_at: Optional[str] = None

    model_snapshot: Dict[str, Any] = Field(default_factory=dict)
    dataset_snapshot: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)

    project: Optional[Any] = Field(default=None, exclude=True, repr=False)

    def storage_key(self) -> str:
        """Deterministic registry key: model + dataset, disambiguated by year/window."""
        key = f"{self.model_key}__{self.dataset_name}"
        if self.year is not None:
            key += f"_y{self.year}"
        if self.window is not None:
            key += f"_w{self.window}"
        return key

    def add_to_project(
        self,
        project: Optional[Any] = None,
        key: Optional[str] = None,
        auto_save: bool = True,
    ) -> "Prediction":
        """Register this prediction on a project. Returns self for chaining."""
        target = project or self.project
        if target is None:
            raise ValueError(
                "Cannot add to project: no project provided and self.project is None."
            )
        if self.created_at is None:
            self.created_at = datetime.now().isoformat(timespec="seconds")
        target.add_prediction(self, key=key, auto_save=auto_save)
        return self
