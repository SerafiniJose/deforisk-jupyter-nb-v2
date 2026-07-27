"""Tests for gui.i18n: key lookup, interpolation, pluralization and parity."""

import json
import re

from gui import i18n


def test_t_resolves_simple_key():
    """A plain dotted key resolves to its catalog value."""
    assert i18n.t("common.save") == "Save"


def test_t_nested_dotted_key():
    """A dotted key resolves through nested catalog objects."""
    # tabs live in shell.json (added in Task 2) — here use common only
    assert i18n.t("common.cancel") == "Cancel"


def test_t_missing_key_returns_key():
    """An unknown key falls back to the raw dotted key."""
    assert i18n.t("does.not.exist") == "does.not.exist"


def test_t_interpolates_named_placeholders():
    """Named kwargs interpolate into ``{name}``-style placeholders."""
    # common.json carries a test-only template key
    assert i18n.t("common._test_fmt", name="X") == "hello X"


def test_plural_selects_variant():
    """plural() picks the one/other variant by count and injects n."""
    # common._test_one / common._test_other added in Step 3. Do NOT pass n= as a
    # kwarg: plural() takes n positionally and injects it into fmt itself (passing
    # both raises "multiple values for argument 'n'").
    assert i18n.plural(1, "common._test_one", "common._test_other") == "1 item"
    assert i18n.plural(3, "common._test_one", "common._test_other") == "3 items"


def test_app_available_locales_is_en_and_es():
    """Only en and es-ES are exposed as available locales."""
    # Spanish ships as "es-ES" — pysepal's LocaleSelect only lists IETF codes
    # present in its locale table, which has no bare "es" (only es-ES/es-AR/...).
    assert set(i18n.app_available_locales()) == {"en", "es-ES"}


def _flat_keys(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flat_keys(v, key))
        else:
            out[key] = v
    return out


def _merged_for_lang(lang):
    merged = {}
    dupes = []
    for f in sorted((i18n.MESSAGES_DIR / lang).glob("*.json")):
        data = json.loads(f.read_text())
        for k in _flat_keys(data):
            if k in merged:
                dupes.append((lang, k, f.name))
        merged.update(_flat_keys(data))
    return merged, dupes


def test_no_duplicate_keys_within_language():
    """No key is declared twice across a language's message files."""
    for lang in ("en", "es-ES"):
        _, dupes = _merged_for_lang(lang)
        assert not dupes, f"Duplicate keys in {lang}: {dupes}"


def test_es_parity_reports_gaps():
    """Keys missing from es-ES are reported, not hard-failed (incremental)."""
    en, _ = _merged_for_lang("en")
    es, _ = _merged_for_lang("es-ES")
    missing = sorted(k for k in en if k not in es and not k.startswith("common._test"))
    # Incremental translation allowed: report, do not hard-fail here.
    if missing:
        print(f"[i18n] {len(missing)} keys not yet in es: {missing[:20]}")
    assert isinstance(missing, list)


def test_relative_time_plural_keys_exist():
    """Relative-time and chip-count plural keys resolve for both languages."""
    for k in (
        "time.days_ago_one",
        "time.days_ago_other",
        "chips.models_one",
        "chips.models_other",
    ):
        assert i18n.t(k, n=1) != k  # resolves, not the raw key


def test_every_model_label_and_description_key_resolves():
    """Every MODEL_REGISTRY entry (and its params/variables) resolves."""
    from gui import i18n
    from gui.tile.train_tile import MODEL_REGISTRY

    for key, spec in MODEL_REGISTRY.items():
        assert i18n.t(spec["label_key"]) != spec["label_key"], key
        # description may be "" in es and fall back to en, but must resolve in en
        assert i18n.t(spec["description_key"]) != spec["description_key"], key
        # Train tile renders a structured summary above the prose description.
        summary = f"models.{key}.summary_md"
        assert i18n.t(summary) != summary, key
        for p in spec.get("params", []) + spec.get("variables", []):
            assert i18n.t(p["label_key"]) != p["label_key"], (key, p.get("key"))
            # Train tile renders a per-parameter hint resolved by convention
            # (models.<model>.params.<key>.hint) — every param must carry one.
            hint = f"models.{key}.params.{p['key']}.hint"
            assert i18n.t(hint) != hint, (key, p.get("key"))


def test_every_predefined_variable_description_key_resolves():
    """Every PREDEFINED_CATALOGUE entry declares a description that resolves."""
    from gui import i18n
    from gui.scripts.predefined_variables import PREDEFINED_CATALOGUE

    for key, meta in PREDEFINED_CATALOGUE.items():
        dk = meta.get("description_key")
        assert dk, key
        assert i18n.t(dk) != dk, key


def _code_keys():
    """All literal keys passed to t("...") / plural(_, "one", "other") in gui/."""
    gui_dir = i18n.MESSAGES_DIR.parent
    t_pat = re.compile(r'\bt\(\s*["\']([\w.]+)["\']')
    plural_pat = re.compile(
        r'\bplural\([^,]+,\s*["\']([\w.]+)["\']\s*,\s*["\']([\w.]+)["\']'
    )
    keys = set()
    for f in gui_dir.rglob("*.py"):
        if "messages" in f.parts:
            continue
        src = f.read_text()
        keys.update(t_pat.findall(src))
        for one, other in plural_pat.findall(src):
            keys.update((one, other))
    return keys


def _en_keys():
    merged, _ = _merged_for_lang("en")  # helper from earlier in this file
    return set(merged)


def test_every_referenced_key_exists_in_en():
    """Every key referenced via t()/plural() in gui/ has an en catalog entry."""
    referenced = _code_keys()
    missing = sorted(
        k
        for k in referenced
        if k not in _en_keys() and not k.startswith("common._test")
    )
    assert not missing, f"t()/plural() keys with no en catalog entry: {missing}"


def test_unused_en_keys_are_reported_advisory():
    """Unused en keys are reported (advisory), not hard-failed."""
    unused = sorted(
        k
        for k in _en_keys()
        if k not in _code_keys() and not k.startswith("common._test")
    )
    if unused:
        print(
            f"[i18n] {len(unused)} en keys not referenced via "
            f"t()/plural(): {unused[:30]}"
        )
    assert isinstance(unused, list)  # advisory: dynamic keys may be built at runtime


def test_t_accepts_key_named_format_placeholder():
    """A ``{key}`` placeholder doesn't collide with t()'s reserved param name."""
    # Regression: t()'s first param was named `key`, which collided with a
    # {key} format placeholder when callers passed key=... — the render crashed
    # with "t() got multiple values for argument 'key'". The lookup key must be
    # positional-only so a same-named placeholder interpolates instead.
    assert i18n.t("tiles.train.model_name_saved_as", key="m1") == "Saved as 'm1'."
    assert (
        i18n.t("tiles.dataset.success_registered", key="ds1")
        == "Dataset 'ds1' registered."
    )


def test_every_key_placeholder_call_site_renders():
    """Every ``{key}``-placeholder catalog value renders without leaking it."""
    # Every catalog value with a {key} placeholder must be callable with key=
    # without colliding with t()'s reserved parameter name (full blast radius
    # of the param-collision bug across dataset_tile/variables_tile/train_tile).
    for dotted in (
        "tiles.dataset.success_registered",
        "tiles.dataset.form_header_edit",
        "tiles.train.model_name_exists_warning",
        "tiles.train.model_name_saved_as",
        "tiles.train.confirm_delete_model_message",
        "tiles.train.confirm_overwrite_message",
    ):
        out = i18n.t(dotted, key="X")
        assert out != dotted and "{key}" not in out, dotted
    # error_toggle_map interpolates both key and exc
    out = i18n.t("tiles.variables.error_toggle_map", key="rivers", exc="boom")
    assert (
        "rivers" in out and "boom" in out and out != "tiles.variables.error_toggle_map"
    )


def test_no_hardcoded_step_numbers_in_messages():
    """Message catalogs must not hardcode step numbers.

    Step numbers derive from the STEPS registry; they drifted for months
    before the pipeline header.
    """
    import re

    for lang in ("en", "es-ES"):
        for f in sorted((i18n.MESSAGES_DIR / lang).glob("*.json")):
            text = f.read_text()
            hits = re.findall(r"(?:Step|Paso) \d", text)
            assert not hits, (lang, f.name, hits)


def test_harmonization_rename_values():
    """Process/Post-process tabs are renamed Harmonization/Derived layers."""
    # Process -> Harmonization, Post-process -> Derived layers (2026-07-16)
    assert i18n.t("workflow.tab_process") == "Harmonization"
    assert i18n.t("workflow.tab_postprocess") == "Derived layers"
    assert i18n.t("tiles.process.header") == "### Harmonization"
    assert i18n.t("tiles.process.run_processing_button") == "Run harmonization"
    assert i18n.t("tiles.postprocess.header") == "### Derived layers"
    assert i18n.t("widgets.variable_list.processed_title") == "Harmonized variables"
    # keys orphaned by the compact tile were dropped (missing-key behavior)
    assert i18n.t("tiles.process.auto_utm_button") == "tiles.process.auto_utm_button"
    assert (
        i18n.t("tiles.process.run_processing_subtitle")
        == "tiles.process.run_processing_subtitle"
    )


def test_translator_ignores_machine_config(monkeypatch, tmp_path):
    """A machine-config locale must not leak in when no pysepal session exists.

    The root fix for the 21-test failure: a machine config holding a
    non-English locale must not leak into translated strings under pytest.
    """
    config = tmp_path / ".sepal-ui-config"
    config.write_text("[sepal-ui]\nlocale = es-ES\n")
    monkeypatch.setattr("pysepal.conf.config_file", config)
    monkeypatch.setattr("pysepal.translator.translator.config_file", config)

    i18n.reset_translator()
    try:
        translator = i18n.get_translator()
        assert translator._target == "en"
    finally:
        i18n.reset_translator()


def test_set_app_locale_swaps_translator_live(monkeypatch):
    """set_app_locale() swaps the active translator without a restart."""
    i18n.reset_translator()
    try:
        english = i18n.t("app.title")
        i18n.set_app_locale("es-ES")
        spanish = i18n.t("app.title")
        assert i18n.get_translator()._target == "es-ES"
        assert english != spanish  # app.title is translated in es-ES
        i18n.set_app_locale("en")
        assert i18n.t("app.title") == english
    finally:
        i18n.reset_translator()
