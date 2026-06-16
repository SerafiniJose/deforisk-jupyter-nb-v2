"""GLM risk model using sklearn LogisticRegression with Patsy formulas."""

from pathlib import Path
from typing import Optional, Union

import numpy as np

from spatialrisk.mlmodels.base import BaseRiskModel


class GLMModel(BaseRiskModel):
    """Logistic Regression risk model with Patsy formula support.

    Attributes
    ----------
    solver : str
        sklearn LogisticRegression solver (default: "lbfgs").
    max_iter : int
        Maximum number of solver iterations (default: 1000).
    random_seed : int, optional
        Random seed for reproducibility.
    """

    model_type: str = "glm"
    solver: str = "lbfgs"
    max_iter: int = 1000
    random_seed: Optional[int] = None

    def fit(
        self,
        formula: Optional[str] = None,
        folder: Optional[Union[str, Path]] = None,
    ) -> "GLMModel":
        """Train a logistic regression model.

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
        from sklearn.linear_model import LogisticRegression
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

        print(f"\n🔧 Training GLM ({self.solver}, max_iter={self.max_iter})...")

        y, x = dmatrices(self.formula, df, NA_action="drop")
        # self._x_design_info = x.design_info

        clf = LogisticRegression(
            solver=self.solver,
            max_iter=self.max_iter,
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
            f"✓ GLM trained — {self.n_samples:,} samples, "
            f"deviance={self.deviance:.2f}, trained_at={self.trained_at}"
        )

        self.save(folder=folder)
        return self

    # apply() is inherited from BaseRiskModel (default _predict_block uses
    # self._ml_model.predict_proba); GLM needs no override.
