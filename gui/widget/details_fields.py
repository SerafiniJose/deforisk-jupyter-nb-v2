"""Read-only field primitives shared by the product details dialogs.

The Train and Sampling tabs each show a stored artifact as a read-only mirror
of the form that created it. The field look and the value formatting live here
so the two dialogs cannot drift apart.
"""

import reacton.ipyvuetify as rv

from gui.i18n import t


def format_value(value) -> str:
    """Display form of a stored parameter value.

    Booleans are translated — a raw ``"True"`` reads as an untranslated leak —
    and that check must come before any numeric handling because ``bool`` is a
    subclass of ``int``. Lists join like the form's multi-value fields.
    """
    if value is None:
        return "—"
    if isinstance(value, bool):
        return t("common.yes") if value else t("common.no")
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value)
    return str(value)


def ro_field(label: str, value) -> None:
    """One read-only field in a details dialog (dense outlined, no message strip)."""
    rv.TextField(
        label=label,
        v_model=format_value(value),
        readonly=True,
        dense=True,
        outlined=True,
        hide_details=True,
        class_="mb-2",
    )
