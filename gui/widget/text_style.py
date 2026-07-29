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

MUTED = "opacity: 0.7;"

__all__ = ["MUTED"]
