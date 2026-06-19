"""SampleSet — a persisted, materialized sample drawn from a Dataset.

A SampleSet runs the Dataset sampling pipeline once and stores the result as
two artifacts on disk:

* a training-table CSV (``table_path``) — exactly the dataframe
  ``Dataset.to_dataframe()`` produces (target column + feature columns +
  cell_id/trial), consumed at fit time;
* a points GeoPackage (``points_path``) — one point per sampled pixel centre,
  carrying every feature column plus a canonical integer ``target`` column
  (1 = event / deforestation, 0 = forest) for map styling.

Solara-free: heavy geo deps are imported lazily inside methods.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from spatialrisk.sampling import Sampling


class SampleSet(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    # Live Project back-reference. Excluded from serialization (mirrors Dataset).
    project: Any = Field(default=None, repr=False, exclude=True)

    dataset_name: str
    target_name: Optional[str] = None
    feature_names: List[str] = Field(default_factory=list)
    year: Optional[int] = None

    strategy: str = "random"
    n_samples: Optional[int] = 10000
    seed: Optional[int] = None

    table_path: Optional[Path] = None
    points_path: Optional[Path] = None

    n_total: int = 0
    n_event: int = 0
    n_forest: int = 0
    created_at: Optional[str] = None

    def model_dump(self, **kwargs):
        # Force-exclude the live back-reference even if a caller forgets to.
        kwargs.setdefault("exclude", set())
        if isinstance(kwargs["exclude"], set):
            kwargs["exclude"] = kwargs["exclude"] | {"project"}
        return super().model_dump(**kwargs)

    def _resolve_dataset(self) -> Any:
        if self.project is None:
            raise ValueError("SampleSet has no project reference.")
        ds = self.project.datasets.get(self.dataset_name)
        if ds is None:
            raise ValueError(
                f"Source dataset '{self.dataset_name}' not found in project."
            )
        return ds

    def generate(self) -> "SampleSet":
        """Run sampling once and write the CSV table + points GeoPackage."""
        import geopandas as gpd
        import rasterio
        from shapely.geometry import Point

        dataset = self._resolve_dataset()

        # Denormalize metadata from the source dataset.
        self.target_name = dataset.target.name
        self.feature_names = [f.name for f in dataset.features]
        if dataset.year is not None:
            self.year = dataset.year

        # Reuse the existing, verified sampling pipeline. adapt=False keeps the
        # legacy strategy from requiring a pixel-area value.
        sampling = Sampling(
            strategy=self.strategy, n_samples=self.n_samples,
            seed=self.seed, adapt=False,
        )
        if self.table_path is not None:
            Path(self.table_path).parent.mkdir(parents=True, exist_ok=True)
        df = dataset.to_dataframe(sampling=sampling, output_csv=self.table_path)

        target_col = dataset.target.name

        # Decode cell_id (= row * ncols + col) to pixel-centre coordinates using
        # the target raster's transform/CRS.
        with rasterio.open(dataset.target.path) as src:
            ncols = src.width
            transform = src.transform
            crs = src.crs
        rows = df["cell_id"].to_numpy() // ncols
        cols = df["cell_id"].to_numpy() % ncols
        xs, ys = rasterio.transform.xy(transform, rows, cols, offset="center")
        geometry = [Point(x, y) for x, y in zip(xs, ys)]

        attrs = {"target": df[target_col].astype(int).to_numpy()}
        for name in self.feature_names:
            if name in df.columns:
                attrs[name] = df[name].to_numpy()
        gdf = gpd.GeoDataFrame(attrs, geometry=geometry, crs=crs)
        if self.points_path is not None:
            Path(self.points_path).parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(self.points_path, driver="GPKG")

        # Summary stats.
        self.n_total = int(len(df))
        self.n_event = int((df[target_col] == 1).sum())
        self.n_forest = int((df[target_col] == 0).sum())
        self.created_at = datetime.now().isoformat(timespec="seconds")
        return self

    def load_table(self) -> pd.DataFrame:
        """Read the materialized training table back into a DataFrame."""
        if self.table_path is None or not Path(self.table_path).exists():
            raise FileNotFoundError(
                f"Sample table not found for '{self.name}': {self.table_path}"
            )
        return pd.read_csv(self.table_path)

    def register(self, project: Any, key: Optional[str] = None,
                 auto_save: bool = True) -> None:
        """Register this sample set with a project."""
        project.add_sample_set(self, key=key, auto_save=auto_save)
