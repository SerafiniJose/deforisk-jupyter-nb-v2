"""Failed allocation jobs are editable: reopen the form prefilled and rerun.

A failed run used to be a dead end — the row carried no actions at all, so the
user could neither retry it nor clear it. Now the job dict keeps the
AllocationForm it was launched from, the failed row offers edit and dismiss,
and the allocation form can seed itself from that entry.
"""

import ipyvuetify as vw
import reacton

from gui.i18n import t

# See test_manage_projects_render: warm the translator before the first render.
t("common.cancel")

from gui.scripts.allocation_runner import (  # noqa: E402
    AllocationForm,
    BordersSelection,
)
from gui.widget.allocation_list import AllocationList  # noqa: E402


def _find(widget, cls, out=None):
    out = [] if out is None else out
    if isinstance(widget, cls):
        out.append(widget)
    for child in getattr(widget, "children", []) or []:
        if hasattr(child, "children") or isinstance(child, cls):
            _find(child, cls, out)
    return out


def _icon_button(box, icon):
    for btn in _find(box, vw.Btn):
        if any(icon in str(i.children) for i in _find(btn, vw.Icon)):
            return btn
    return None


def _entry(**kw):
    """The AllocationForm a job was launched with."""
    base = dict(
        name="allocation_1",
        prediction_key="icar_run",
        user_defrate_path=None,
        borders=BordersSelection(method="FILE", file_path="/data/borders.gpkg"),
        mask_file=None,
        defor_juris_ha=20000.0,
        years_forecast=4.0,
        density_map=False,
    )
    base.update(kw)
    return AllocationForm(**base)


def _job_row(status="failed", entry=None, job_id="j1"):
    return {
        "kind": "job",
        "key": job_id,
        "job_id": job_id,
        "name": "allocation_1",
        "status": status,
        "error": "boom" if status == "failed" else None,
        "entry": _entry() if entry is None else entry,
    }


def _render_list(rows, on_edit=None, on_dismiss=None):
    box, _rc = reacton.render(
        AllocationList(
            rows=rows,
            on_delete=lambda key: None,
            on_edit=on_edit,
            on_dismiss=on_dismiss,
        )
    )
    return box


def test_failed_row_offers_edit_that_hands_back_the_row():
    """The pencil on a failed row hands the whole row (job_id + entry) up."""
    edited = []
    box = _render_list([_job_row()], on_edit=edited.append)
    btn = _icon_button(box, "mdi-pencil-outline")
    assert btn is not None
    btn.fire_event("click", {})
    assert edited and edited[0]["job_id"] == "j1"
    assert edited[0]["entry"].name == "allocation_1"


def test_running_row_offers_neither_action():
    """A run in flight cannot be edited or dismissed out from under its worker."""
    box = _render_list(
        [_job_row(status="running")],
        on_edit=lambda row: None,
        on_dismiss=lambda i: None,
    )
    assert _icon_button(box, "mdi-pencil-outline") is None
    assert _icon_button(box, "mdi-close") is None


def test_failed_row_without_an_entry_offers_dismiss_only():
    """Defensive: a job that never recorded its entry can still be cleared."""
    box = _render_list(
        [_job_row(entry=False)], on_edit=lambda row: None, on_dismiss=lambda i: None
    )
    assert _icon_button(box, "mdi-pencil-outline") is None
    assert _icon_button(box, "mdi-close") is not None


def test_failed_row_dismiss_hands_back_the_job_id():
    """Dismiss passes the job id, not the row."""
    dismissed = []
    box = _render_list([_job_row()], on_dismiss=dismissed.append)
    _icon_button(box, "mdi-close").fire_event("click", {})
    assert dismissed == ["j1"]
