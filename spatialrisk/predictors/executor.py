"""Session-aware fit/apply executor.

Reuses the legacy mlmodels fit() implementations verbatim (no retraining
logic here). Drives them from the picklable *FitSpec / *ApplySpec built by
ProjectSession, captures estimators via the EstimatorStore, and writes back
trained ModelSpecs / PredictionSpecs.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class _Var:
    name: str
    path: str


@dataclass
class _DatasetShim:
    """Minimal dataset duck-type for BaseRiskModel._prepare_samples /
    Dataset.to_dataframe — carries only resolved paths + sampling metadata."""
    name: str
    year: Optional[int]
    target: _Var
    features: List[_Var] = field(default_factory=list)

    def validate(self) -> bool:
        return self.target is not None and bool(self.features)

    def to_dataframe(self, sampling=None, output_csv=None, **kwargs):
        from spatialrisk.dataset import Dataset
        # Bind the real to_dataframe algorithm to this duck-typed object.
        # In Python 3 the class attribute is already a plain function, so we
        # call it directly with ``self`` as the first positional argument.
        return Dataset.to_dataframe(self, sampling=sampling,
                                    output_csv=output_csv, **kwargs)
