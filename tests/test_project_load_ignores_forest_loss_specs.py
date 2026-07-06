"""Old project JSONs with a forest_loss_specs key must load cleanly."""

import importlib
import json


def test_load_ignores_legacy_forest_loss_specs(tmp_path, monkeypatch):
    monkeypatch.setenv("SPATIAL_RISK_DATA_DIR", str(tmp_path))
    import spatialrisk.project as proj_mod
    importlib.reload(proj_mod)

    p = proj_mod.Project(project_name="t")
    p.save()

    manifest = next((tmp_path / "t").glob("*_project.json"))
    data = json.loads(manifest.read_text())
    data["forest_loss_specs"] = [
        {
            "name": "forest_loss_2015_2020",
            "start_key": "forest_gfc_2015",
            "end_key": "forest_gfc_2020",
            "start_year": 2015,
            "end_year": 2020,
            "tags": ["deforestation", "forest_loss"],
        }
    ]
    manifest.write_text(json.dumps(data))

    loaded = proj_mod.Project.load(project_name="t")
    assert not hasattr(loaded, "forest_loss_specs")
