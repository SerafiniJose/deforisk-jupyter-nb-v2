"""Persisted record of one Step 8 evaluation run (one truth vs N maps).

A plain pydantic model with NO live ``project`` back-reference, so it serializes
without recursion and needs no ``_relink_backrefs`` handling. The full output
table is stored inline in ``indices`` so the GUI popup reads it straight from the
record — robust across save/load even if a later same-truth run overwrites the
on-disk ``indices_all.csv``.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EvaluationRecord(BaseModel):
    name: Optional[str] = None
    truth_tag: str
    truth_defor: str
    truth_forest: str
    time_interval: int
    prediction_keys: List[str] = Field(default_factory=list)
    csizes: List[int] = Field(default_factory=list)
    metrics: List[str] = Field(default_factory=list)  # shown index columns; [] = all
    created_at: str
    indices: List[Dict[str, Any]] = Field(default_factory=list)
    csv_path: Optional[str] = None
    run_id: str

    def storage_key(self) -> str:
        """Deterministic, history-safe registry key: truth + timestamp + run id."""
        compact = self.created_at.replace("-", "").replace(":", "").replace("T", "")
        return f"{self.truth_tag}__{compact}_{self.run_id}"
