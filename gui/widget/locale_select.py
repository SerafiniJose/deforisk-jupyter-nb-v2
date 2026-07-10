"""Language selector that actually persists the chosen locale across reloads.

pysepal 3.6.2's ``LocaleSelect`` never writes the picked language to
``~/.sepal-ui-config``: its template pushes the value to Python with
``this.$emit("update:selected_locale", code)`` — the Vue ``.sync`` convention
that only fires when a *parent* component binds ``:selected_locale.sync``. As a
standalone root widget it has no such parent, so the emit is dropped, the
``selected_locale`` trait never updates, the ``_on_locale_select`` observer
never runs, and no ``locale`` key is ever written. On reload
``Translator.find_target`` then falls back to ``"en"`` — the "language resets to
English after refresh" bug.

``AppLocaleSelect`` swaps in an app-owned template (``locale_select.vue``) whose
only change is to assign the synced trait directly (``this.selected_locale =
code``), like every other pysepal widget. It also seeds ``selected_locale`` from
the saved config on construction so the button flag reflects the persisted
language after a reload.
"""

from configparser import ConfigParser
from pathlib import Path

from pysepal.conf import config_file
from pysepal.sepalwidgets.vue_app import LocaleSelect
from traitlets import Unicode

_TEMPLATE = str(Path(__file__).parent / "locale_select.vue")


class AppLocaleSelect(LocaleSelect):
    """``LocaleSelect`` that persists the selection and restores it on reload."""

    template_file = Unicode(_TEMPLATE).tag(sync=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Seed from the persisted locale so the widget shows the saved language
        # after a reload (upstream always starts at "en"). Only apply it if it
        # is an offered locale; assigning triggers _on_locale_select, which
        # idempotently re-writes the same value to config.
        saved = self._saved_locale()
        offered = {loc["code"] for loc in self.available_locales}
        if saved in offered and saved != self.selected_locale:
            self.selected_locale = saved

    @staticmethod
    def _saved_locale() -> str:
        """Read ``[sepal-ui] locale`` from the config, mirroring pysepal's own
        read path (``Translator.find_target``); default ``"en"``."""
        if config_file.is_file():
            config = ConfigParser()
            config.read(config_file)
            return config.get("sepal-ui", "locale", fallback="en")
        return "en"
