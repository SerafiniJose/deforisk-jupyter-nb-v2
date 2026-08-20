"""Regression test: a single-year variable must resolve and must not crash
dataset validation.

A variable carrying a year is stored under the key ``f"{name}_{year}"`` (see
``LocalRasterVar.add_as_processed``). ``is_temporal`` only reports ``True`` for
2+ distinct years, so a variable with a *single* year (e.g. ``towns`` keyed
``towns_2020`` in the MTQ/SMR projects) is treated as non-temporal. The dataset
tile then resolves it via ``get_variable(name, year=None)``, which looked up the
bare ``name`` key, missed, and returned ``None``. That ``None`` flowed into
``Dataset.features`` and ``Dataset.validate`` blew up with
``'NoneType' object has no attribute 'name'``.

These lock in that ``get_variable`` resolves a single-year variable without a
year argument, and that the full dataset validate flow no longer crashes.
"""

from spatialrisk.dataset import Dataset
from spatialrisk.project import Project
from spatialrisk.variables import LocalRasterVar

Project._ensure_model_schemas()


def _project_with_single_year_var(tmp_path):
    """forest_gfc: temporal (2 years); towns: single-year (towns_2020);
    altitude: truly static. All point at real (empty) files so validate() can
    confirm existence."""
    p = Project(project_name="t")

    def _var(name, year):
        key = f"{name}_{year}" if year else name
        f = tmp_path / f"{key}.tif"
        f.write_bytes(b"")  # validate() only checks path.exists(), not contents
        return key, LocalRasterVar.model_construct(name=name, project=p, year=year, path=f)

    for name, year in [("forest_gfc", 2020), ("forest_gfc", 2024),
                       ("towns", 2020), ("altitude", None)]:
        key, var = _var(name, year)
        p.processed_variables[key] = var
    return p


def test_get_variable_resolves_single_year_without_year_arg(tmp_path):
    """The root cause: bare-name lookup must fall back to the sole instance."""
    p = _project_with_single_year_var(tmp_path)
    assert p.is_temporal("towns") is False  # single year -> non-temporal
    var = p.get_variable("towns", year=None)
    assert var is not None
    assert var.name == "towns"
    assert var.year == 2020


def test_get_variable_temporal_without_year_is_ambiguous(tmp_path):
    """A genuinely temporal variable looked up without a year stays None
    (the caller must disambiguate)."""
    p = _project_with_single_year_var(tmp_path)
    assert p.get_variable("forest_gfc", year=None) is None
    assert p.get_variable("forest_gfc", year=2024).year == 2024


def test_dataset_validate_with_single_year_feature(tmp_path):
    """Mirrors the dataset tile's on_validate with a single-year feature."""
    p = _project_with_single_year_var(tmp_path)

    ds = Dataset(project=p, name="calib", year=2020)
    target_is_temporal = p.is_temporal("forest_gfc")
    ds.set_target("forest_gfc", year=2020 if target_is_temporal else None)
    ds.set_features(["altitude", "towns"])  # 'towns' is the single-year trap

    assert None not in ds.features
    assert ds.validate() is True
