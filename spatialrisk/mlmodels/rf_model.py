"""Random Forest risk model using sklearn with Patsy formulas."""

from pathlib import Path
from typing import Optional, Union

import numpy as np

from spatialrisk.mlmodels.base import BaseRiskModel


class RFModel(BaseRiskModel):
    """Random Forest risk model with Patsy formula support.

    Attributes
    ----------
    n_trees : int
        Number of decision trees (default: 100).
    max_depth : int
        Maximum tree depth (default: 15).
    min_samples_leaf : int
        Minimum samples per leaf node (default: 2).
    random_seed : int, optional
        Random seed for reproducibility.
    """

    model_type: str = "rf"
    n_trees: int = 100
    max_depth: int = 15
    min_samples_leaf: int = 2
    random_seed: Optional[int] = None

    def fit(
        self,
        formula: Optional[str] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "RFModel":
        """Train a Random Forest classifier.

        Parameters
        ----------
        formula : str, optional
            Patsy formula. If omitted, falls back to self.formula or
            auto-generates via generate_patsy_formula(self.dataset).
        folder : str or Path, optional
            Folder for saving the model pickle. Defaults to project model folder.

        Returns
        -------
        self
        """
        from patsy import dmatrices
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import log_loss

        # Auto-save full training CSV if samples_path not already set
        if self.samples_path is None:
            _folder = (
                Path(folder)
                if folder is not None
                else (self._default_folder() or Path.cwd())
            )
            Path(_folder).mkdir(parents=True, exist_ok=True)
            _csv = (
                Path(_folder) / f"samples_{self.model_type}_{self.name or 'model'}.csv"
            )
        else:
            _csv = None

        df, formula = self._prepare_samples(formula, output_csv=_csv)

        print(
            f"\n🔧 Training Random Forest "
            f"(n_trees={self.n_trees}, max_depth={self.max_depth})..."
        )

        df = df.dropna()
        y, x = dmatrices(self.formula, df, NA_action="drop")
        self._x_design_info = x.design_info

        clf = RandomForestClassifier(
            n_estimators=self.n_trees,
            max_depth=self.max_depth,
            min_samples_leaf=self.min_samples_leaf,
            n_jobs=-1,
            random_state=self.random_seed,
        )
        y_arr = np.asarray(y)[:, 0]
        x_arr = np.asarray(x)
        clf.fit(x_arr, y_arr)
        self._ml_model = clf

        # Training metrics
        self.n_samples = len(df)
        y_pred = clf.predict_proba(x_arr)[:, 1]
        self.deviance = 2.0 * log_loss(y_arr, y_pred, normalize=False)

        self._stamp_now()
        self.trained = True
        print(
            f"✓ RF trained — {self.n_samples:,} samples, "
            f"deviance={self.deviance:.2f}, trained_at={self.trained_at}"
        )

        self.save(folder=folder)
        return self

    # apply() is inherited from BaseRiskModel (default _predict_block uses
    # self._ml_model.predict_proba); RF needs no override.
