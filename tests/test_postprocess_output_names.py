"""Pure output-name preview helpers for the Post-process dialog."""
from types import SimpleNamespace

from gui.scripts.process_actions import change_output_name, postprocess_output_name


def _project(**processed):
    return SimpleNamespace(processed_variables=processed)


def _var(name, year=None):
    return SimpleNamespace(name=name, year=year)


def test_change_name_same_source():
    p = _project(a2010=_var("forest", 2010), a2020=_var("forest", 2020))
    assert change_output_name(p, "loss", "a2010", "a2020") == "loss_forest_2010_2020"


def test_change_name_cross_source():
    p = _project(a=_var("tmf", 2010), b=_var("gfc", 2020))
    assert change_output_name(p, "gain", "a", "b") == "gain_tmf_2010_gfc_2020"


def test_change_name_invalid_inputs_return_none():
    p = _project(a=_var("tmf", 2010), b=_var("gfc", 2020), s=_var("static", None))
    assert change_output_name(p, "loss", "missing", "b") is None
    assert change_output_name(p, "loss", "s", "b") is None          # non-temporal
    assert change_output_name(p, "loss", "b", "a") is None          # start >= end


def test_postprocess_name_appends_step_to_var_name():
    p = _project(k=_var("loss_forest_2010_2020"))
    assert postprocess_output_name(p, "k", "edge") == "loss_forest_2010_2020_edge"
    assert postprocess_output_name(p, "k", "dist") == "loss_forest_2010_2020_dist"
    assert postprocess_output_name(p, "missing", "edge") is None
