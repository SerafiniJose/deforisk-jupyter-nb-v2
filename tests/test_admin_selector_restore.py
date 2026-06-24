"""Regression test for restoring a multi-level admin AOI cascade.

Reproduces the bug where loading a project whose AOI was an ADMIN1 selection
(e.g. Paraguay > Amambay) left both cascade dropdowns empty: AoiView binds the
selector's restore seed ``initial`` to the live ``admin_code`` reactive, which
the selector itself resets to ``None`` (via update_output) on mount before the
async cascade seeds — wiping the restore chain. See gui/widget/admin_selector.py.
"""

import time

import solara
import pysepal.solara.components.aoi.admin as admin_mod
from gui.widget.admin_selector import AdminLevelSelector


def _fake_fetch(level, parent_code):
    # Paraguay (206) > Amambay (2184); mirrors fetch_admin_items' item shape.
    if level == 0:
        return [{"text": "Paraguay", "value": "206"}]
    if level == 1 and str(parent_code) == "206":
        return [{"text": "Amambay", "value": "2184"}]
    return []


@solara.component
def _RestoreHarness(admin_code):
    # Mirrors AoiView: the restore seed `initial` is bound live to the same
    # reactive the selector drives (gui/widget/aoi_view.py).
    AdminLevelSelector(
        method="ADMIN1",
        gee=True,
        value=admin_code,
        initial=admin_code.value,
    )


def _settle(admin_code, expected, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline and admin_code.value != expected:
        time.sleep(0.05)
    # Give a redundant double-run a chance to wrongly wipe the value before asserting.
    time.sleep(0.5)


def test_admin1_cascade_restores_seeded_value(monkeypatch):
    monkeypatch.setattr(admin_mod, "fetch_admin_items", _fake_fetch)

    # The restored final code is present at mount (load_aoi set it before remount).
    admin_code = solara.reactive("2184")
    box, rc = solara.render(_RestoreHarness(admin_code), handle_error=False)
    try:
        _settle(admin_code, "2184")
        assert admin_code.value == "2184", (
            f"cascade failed to restore admin code; admin_code={admin_code.value!r}"
        )
    finally:
        rc.close()


def _fake_fetch_admin2(level, parent_code):
    # Algeria (101) > Adrar (1001) > Adrar (100001).
    mapping = {
        (0, ""): [{"text": "Algeria", "value": "101"}],
        (1, "101"): [{"text": "Adrar", "value": "1001"}],
        (2, "1001"): [{"text": "Adrar", "value": "100001"}],
    }
    return mapping.get((level, str(parent_code)), [])


@solara.component
def _RestoreHarness2(admin_code):
    AdminLevelSelector(
        method="ADMIN2", gee=True, value=admin_code, initial=admin_code.value
    )


def test_admin2_cascade_restores_full_chain(monkeypatch):
    monkeypatch.setattr(admin_mod, "fetch_admin_items", _fake_fetch_admin2)

    admin_code = solara.reactive("100001")
    box, rc = solara.render(_RestoreHarness2(admin_code), handle_error=False)
    try:
        _settle(admin_code, "100001")
        assert admin_code.value == "100001", (
            f"ADMIN2 cascade failed to restore; admin_code={admin_code.value!r}"
        )
    finally:
        rc.close()
