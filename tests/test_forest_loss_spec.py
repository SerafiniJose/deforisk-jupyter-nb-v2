from spatialrisk.project import Project
from spatialrisk.variables.models import ForestLossSpec


def test_forest_loss_spec_defaults():
    spec = ForestLossSpec(
        name="forest_loss_2015_2020",
        start_key="forest_gfc_2015",
        end_key="forest_gfc_2020",
        start_year=2015,
        end_year=2020,
    )
    assert spec.tags == ["deforestation", "forest_loss"]


def test_project_forest_loss_specs_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("SPATIAL_RISK_DATA_DIR", str(tmp_path))
    # Reload module-level DATA_DIR so save/load use tmp_path.
    import importlib
    import spatialrisk.project as proj_mod
    importlib.reload(proj_mod)
    from spatialrisk.variables.models import ForestLossSpec as Spec

    p = proj_mod.Project(project_name="t")
    p.forest_loss_specs.append(
        Spec(
            name="forest_loss_2015_2020",
            start_key="forest_gfc_2015",
            end_key="forest_gfc_2020",
            start_year=2015,
            end_year=2020,
        )
    )
    p.save()
    loaded = proj_mod.Project.load(project_name="t")
    assert len(loaded.forest_loss_specs) == 1
    assert loaded.forest_loss_specs[0].name == "forest_loss_2015_2020"
    assert loaded.forest_loss_specs[0].start_key == "forest_gfc_2015"
