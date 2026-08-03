"""Legacy 'benchmark_*' model keys (old GUI family token) migrate to 'jnr_*'.

Before the family token was unified on model_type, the GUI registered JNR
benchmark models under 'benchmark[_name]' while every family-dispatching code
path expects 'jnr[_name]'. Old project JSONs still carry those keys; load()
renames them so existing trained models keep working.
"""

import json


def test_load_renames_legacy_benchmark_model_keys(tmp_path, monkeypatch):
    """'benchmark[_name]' keys come back as 'jnr[_name]'; 'jnr_*' keys stay."""
    import spatialrisk.project as proj_mod

    monkeypatch.setattr(proj_mod, "downloads_folder", tmp_path)

    p = proj_mod.Project(project_name="t")
    p.save()

    manifest = next((tmp_path / "t").glob("*_project.json"))
    data = json.loads(manifest.read_text())
    data["models"] = {
        "benchmark_v1": {"model_type": "jnr", "name": "v1"},
        "benchmark": {"model_type": "jnr"},
        "jnr_kept": {"model_type": "jnr", "name": "kept"},
    }
    manifest.write_text(json.dumps(data))

    loaded = proj_mod.Project.load(project_name="t")

    assert set(loaded.models) == {"jnr_v1", "jnr", "jnr_kept"}


def test_load_does_not_touch_other_benchmark_prefixed_keys(tmp_path, monkeypatch):
    """Only keys whose model_type is 'jnr' are renamed.

    A custom key on a different family that happens to start with
    'benchmark' is left alone.
    """
    import spatialrisk.project as proj_mod

    monkeypatch.setattr(proj_mod, "downloads_folder", tmp_path)

    p = proj_mod.Project(project_name="t2")
    p.save()

    manifest = next((tmp_path / "t2").glob("*_project.json"))
    data = json.loads(manifest.read_text())
    data["models"] = {"benchmark_glm": {"model_type": "glm", "name": "glm"}}
    manifest.write_text(json.dumps(data))

    loaded = proj_mod.Project.load(project_name="t2")

    assert set(loaded.models) == {"benchmark_glm"}
