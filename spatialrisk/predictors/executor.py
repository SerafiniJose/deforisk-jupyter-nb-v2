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


class SessionExecutor:
    """Drives legacy mlmodels fit/apply from Session specs (the missing wiring)."""

    def fit(self, session, model_key, **kw):
        spec = session.fit_spec(model_key)
        if spec.model_type in ("glm", "rf"):
            return self._fit_supervised(session, model_key, spec)
        if spec.model_type == "icar":
            return self._fit_icar(session, model_key, spec)
        if spec.model_type in ("jnr", "mw"):
            return self._fit_benchmark(session, model_key, spec)
        raise NotImplementedError(f"fit: unsupported model_type {spec.model_type!r}")

    # ---- GLM / RF -----------------------------------------------------
    def _build_shim(self, spec):
        # The target name is the dataset target column; recover it from the
        # formula LHS (the same column the legacy sampler writes).
        target_name = spec.formula.split("~", 1)[0].strip()
        features = [_Var(name, path) for name, path in spec.feature_paths.items()]
        return _DatasetShim(
            name=spec.model_key, year=None,
            target=_Var(target_name, spec.target_path),
            features=features,
        )

    def _fit_supervised(self, session, model_key, spec):
        import os
        from spatialrisk.mlmodels.glm_model import GLMModel
        from spatialrisk.mlmodels.rf_model import RFModel
        from spatialrisk.document import GLMSpec, RFSpec

        cls = GLMModel if spec.model_type == "glm" else RFModel
        model = cls(name=model_key, sampling=spec.sampling,
                    formula=spec.formula, parameters=dict(spec.parameters))
        model.dataset = self._build_shim(spec)
        # Leave samples_path=None so legacy fit() samples + writes the auto CSV
        # into the spec's folder (filename samples_{model_type}_{name}.csv);
        # the curated EstimatorStore payload + trained ModelSpec are written here.
        model.samples_path = None
        folder = os.path.dirname(str(spec.output_sample_path))
        model.fit(folder=folder)

        payload = {
            "ml_model": model._ml_model,
            "design_sample": getattr(model, "_design_sample", None),
            "formula": model.formula,
            "samples_path": str(model.samples_path),
        }
        est_path = str(spec.estimator_pickle
                       or f"{str(spec.output_sample_path)[:-4]}.pickle")
        ref = session.estimator_store.save(payload, est_path)

        spec_cls = GLMSpec if spec.model_type == "glm" else RFSpec
        new = spec_cls(
            model_type=spec.model_type, name=model_key,
            dataset_name=session._doc.models[model_key].dataset_name,
            formula=model.formula, parameters=dict(spec.parameters),
            sampling=spec.sampling, samples_path=str(model.samples_path),
            feature_names=tuple(spec.feature_paths.keys()),
            trained=True, trained_at=model.trained_at,
            n_samples=model.n_samples, deviance=model.deviance,
            estimator_pickle=ref,
        )
        session.register_model(new, key=model_key)
        return new
