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
    tags: tuple = ()


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


@dataclass
class _BenchmarkShim:
    """Dataset duck-type for ``JNRBenchmarkModel.fit`` / ``MWModel.fit``.

    The legacy benchmark fits read the dataset's ``target`` (path + name +
    ``deforestation`` tag) and named features (``forest_edge`` always;
    ``forest`` for MW), plus ``name`` / ``year``. We mirror that shape from the
    resolved spec paths. Feature names match the model defaults
    (``forest_edge_var="forest_edge"``, ``forest_var="forest"``) so the legacy
    ``_get_feature`` lookups succeed without overriding those mappings.
    """
    defor_file: str
    forest_edge_file: str
    forest_file: Optional[str] = None

    def __post_init__(self):
        # JNRBenchmarkModel.fit validates the target carries the
        # 'deforestation' tag; both benchmarks operate on a defor/event target.
        self.target = _Var(name="defor", path=self.defor_file,
                           tags=("deforestation",))
        self.features = [_Var(name="forest_edge", path=self.forest_edge_file)]
        if self.forest_file is not None:
            self.features.append(_Var(name="forest", path=self.forest_file))
        self.name = None
        self.year = None


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
    @staticmethod
    def _target_column(formula):
        """Recover the bare target column name from a Patsy formula LHS.

        Handles both the supervised convention ``defor ~ ...`` and the
        binomial/iCAR convention ``I(defor) + trial ~ ...`` (forestatrisk
        requires the ``I(<target>) + trial`` response), returning the bare
        ``defor`` either way — the column ``dataset.to_dataframe`` writes.
        """
        import re
        lhs = formula.split("~", 1)[0]
        # First term of the LHS (drop the binomial ``+ trial`` companion).
        first = lhs.split("+", 1)[0].strip()
        m = re.match(r"I\((.+)\)$", first)
        return (m.group(1) if m else first).strip()

    def _build_shim(self, spec):
        # The target name is the dataset target column; recover it from the
        # formula LHS (the same column the legacy sampler writes).
        target_name = self._target_column(spec.formula)
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

    # ---- JNR / MW -----------------------------------------------------
    def _fit_benchmark(self, session, model_key, spec):
        from spatialrisk.document import JNRSpec, MWSpec
        base_model = session._doc.models[model_key]

        if spec.model_type == "jnr":
            from spatialrisk.mlmodels.jnr_model import JNRBenchmarkModel
            model = JNRBenchmarkModel(
                name=model_key, defor_threshold=spec.defor_threshold,
                max_dist=spec.max_dist, parameters={})
            model.fit(
                dataset=_BenchmarkShim(defor_file=spec.defor_file,
                                       forest_edge_file=spec.forest_edge_file),
                folder=spec.out_root)
            new = JNRSpec(
                model_type="jnr", name=model_key,
                dataset_name=base_model.dataset_name,
                parameters=dict(base_model.parameters),
                trained=True, trained_at=model.trained_at,
                dist_thresh=float(model.dist_thresh),
                dist_bins=tuple(float(b) for b in model.dist_bins))
        else:
            from spatialrisk.mlmodels.mw_model import MWModel
            model = MWModel(
                name=model_key, defor_threshold=spec.defor_threshold,
                win_size_list=list(spec.win_sizes), parameters={})
            model.fit(
                dataset=_BenchmarkShim(defor_file=spec.defor_file,
                                       forest_edge_file=spec.forest_edge_file,
                                       forest_file=spec.forest_file),
                defor_threshold=spec.defor_threshold,
                time_interval=spec.time_interval, folder=spec.out_root)
            new = MWSpec(
                model_type="mw", name=model_key,
                dataset_name=base_model.dataset_name,
                parameters=dict(base_model.parameters),
                trained=True, trained_at=model.trained_at,
                dist_thresh=float(model.dist_thresh),
                win_size_list=tuple(spec.win_sizes),
                ldefrate_files={str(k): str(v)
                                for k, v in model.ldefrate_files.items()})
        session.register_model(new, key=model_key)
        return new

    # ---- iCAR ---------------------------------------------------------
    def _fit_icar(self, session, model_key, spec):
        from spatialrisk.mlmodels.icar_model import ICARModel
        from spatialrisk.document import ICARSpec

        model = ICARModel(
            name=model_key, sampling=spec.sampling, formula=spec.formula,
            csize=spec.csize, mcmc=spec.mcmc, burnin=spec.burnin, thin=spec.thin,
            prior_vrho=spec.prior_vrho, beta_start=spec.beta_start,
            parameters={}, random_seed=spec.random_seed,
        )
        model.dataset = self._build_shim(spec)
        out_dir = str(spec.rho_path).rsplit("/", 1)[0] if spec.rho_path else \
            str(spec.output_sample_path).rsplit("/", 1)[0]
        model.fit(folder=out_dir)

        payload = {
            "ml_model": model._ml_model, "design_sample": None,
            "formula": model.formula, "samples_path": str(model.samples_path),
        }
        est_path = spec.estimator_pickle or f"{out_dir}/{model_key}.pickle"
        ref = session.estimator_store.save(payload, est_path)

        new = ICARSpec(
            model_type="icar", name=model_key,
            dataset_name=session._doc.models[model_key].dataset_name,
            formula=model.formula, parameters={}, sampling=spec.sampling,
            samples_path=str(model.samples_path),
            feature_names=tuple(spec.feature_paths.keys()),
            trained=True, trained_at=model.trained_at,
            n_samples=model.n_samples, deviance=model.deviance,
            estimator_pickle=ref, rho_path=str(model.rho_path),
        )
        session.register_model(new, key=model_key)
        return new
