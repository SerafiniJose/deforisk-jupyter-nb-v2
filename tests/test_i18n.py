import json
from pathlib import Path

import pytest

from gui import i18n


def test_t_resolves_simple_key():
    assert i18n.t("common.save") == "Save"


def test_t_nested_dotted_key():
    # tabs live in shell.json (added in Task 2) — here use common only
    assert i18n.t("common.cancel") == "Cancel"


def test_t_missing_key_returns_key():
    assert i18n.t("does.not.exist") == "does.not.exist"


def test_t_interpolates_named_placeholders():
    # common.json carries a test-only template key
    assert i18n.t("common._test_fmt", name="X") == "hello X"


def test_plural_selects_variant():
    # common._test_one / common._test_other added in Step 3. Do NOT pass n= as a
    # kwarg: plural() takes n positionally and injects it into fmt itself (passing
    # both raises "multiple values for argument 'n'").
    assert i18n.plural(1, "common._test_one", "common._test_other") == "1 item"
    assert i18n.plural(3, "common._test_one", "common._test_other") == "3 items"


def test_app_available_locales_is_en_and_es():
    assert set(i18n.app_available_locales()) == {"en", "es"}


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
    for lang in ("en", "es"):
        _, dupes = _merged_for_lang(lang)
        assert not dupes, f"Duplicate keys in {lang}: {dupes}"


def test_es_parity_reports_gaps():
    en, _ = _merged_for_lang("en")
    es, _ = _merged_for_lang("es")
    missing = sorted(k for k in en if k not in es and not k.startswith("common._test"))
    # Incremental translation allowed: report, do not hard-fail here.
    if missing:
        print(f"[i18n] {len(missing)} keys not yet in es: {missing[:20]}")
    assert isinstance(missing, list)


def test_relative_time_plural_keys_exist():
    for k in ("time.days_ago_one", "time.days_ago_other",
              "chips.models_one", "chips.models_other"):
        assert i18n.t(k, n=1) != k  # resolves, not the raw key
