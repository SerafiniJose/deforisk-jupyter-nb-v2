"""Shared inline text styles.

``MUTED`` replaces Vuetify's ``text--secondary`` helper class, which is unusable
here. That class carries no colour of its own — it resolves through an ancestor
selector::

    .theme--dark.v-application  .text--secondary { color: hsla(0,0%,100%,.7) }
    .theme--light.v-application .text--secondary { color: rgba(0,0,0,.6) }

(both ``!important``.)

Almost every tile in this app renders inside a ``v-dialog``, and Vuetify
teleports dialog content out of the widget tree into a global overlay
container. Under voila (the SEPAL entry path) that container is built by
jupyter-vuetify's ``createDivs()``, which hard-codes ``theme--light`` on it and
never updates it. So in the dark theme the helper class resolved to near-black
text on a dark card — while the same code was correct under solara-server,
which never lets ``createDivs()`` run.

Dimming by opacity sidesteps the ancestor lookup entirely: the element inherits
the card's own (correctly themed) colour and fades it, so it is right in both
themes and under both hosts. 0.7 matches the dark-theme alpha exactly and the
light-theme one closely (0.7 x 0.87 = 0.61 vs 0.6).
"""

import solara

MUTED = "opacity: 0.7;"

#: CSS class that collapses a field's *empty* Vuetify messages row (rule lives
#: in ``creation_dialog._ADVANCED_PANEL_CSS``). Wrap a field in
#: ``solara.Div(classes=[TIGHT_FIELD])`` whenever a ``FieldHint`` follows it.
TIGHT_FIELD = "sr-tight-field"


@solara.component
def FieldHint(children=[]):
    """A field's own description — sits tight under the field it describes.

    Vuetify reserves a ``.v-text-field__details`` row under every input for
    validation messages. None of the hinted fields in the creation forms use
    ``rules``, so that row is always empty and pushes the description ~22px
    below the field it belongs to. Pair this with a
    ``solara.Div(classes=[TIGHT_FIELD])`` wrapper around that field: the
    wrapper collapses the empty row, and this supplies the spacing it used to
    provide before the next field.
    """
    solara.Column(
        style="margin-top:-2px;margin-bottom:14px;gap:2px;", children=children
    )


__all__ = ["MUTED", "TIGHT_FIELD", "FieldHint"]
