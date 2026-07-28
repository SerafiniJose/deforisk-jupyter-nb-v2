"""Old project JSONs with a forest_loss_specs key must load cleanly."""

import json


def test_load_ignores_legacy_forest_loss_specs(tmp_path, monkeypatch):
    # Redirect the module-level `downloads_folder` rather than reloading the
    # module. save()/load() read it at call time, so this is equivalent — and
    # unlike importlib.reload it is self-restoring. A reload left a *second*
    # Project class in sys.modules (spatialrisk.Project is
    # spatialrisk.project.Project became False) and rebound downloads_folder to
    # a tmp_path pytest then deleted, for the rest of the session.
    import spatialrisk.project as proj_mod

    monkeypatch.setattr(proj_mod, "downloads_folder", tmp_path)

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
