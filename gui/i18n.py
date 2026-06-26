"""Internationalization for the Spatial Risk GUI.

Reuses pysepal's Translator (JSON locale folders with es->en fallback). The
active translator is rebuilt per kernel start from ~/.sepal-ui-config so a page
reload applies a newly selected language. No string may be resolved at import
time — always call t()/plural() inside a component render.
"""

from pathlib import Path

import solara
from pysepal.translator import Translator

MESSAGES_DIR = Path(__file__).parent / "messages"

# Active translator for THIS session. Re-created on kernel start (reload-aware).
_translator = solara.reactive(None)


def get_translator() -> Translator:
    if _translator.value is None:
        _translator.value = Translator(MESSAGES_DIR)  # target="" -> reads config
    return _translator.value


def reset_translator() -> None:
    """Re-read the configured locale. Call from on_kernel_start."""
    _translator.value = Translator(MESSAGES_DIR)


def t(key: str, /, **fmt) -> str:
    """Resolve a dotted key against the active catalog (es->en->key) and
    str.format(**fmt). A key missing in both languages returns the key string
    so a gap degrades visibly instead of crashing the GUI.

    ``key`` is positional-only so a catalog value may carry a ``{key}``
    placeholder passed as a keyword (``key=...``) without colliding with this
    lookup parameter."""
    node = get_translator()
    try:
        for part in key.split("."):
            node = node[part]
        text = str(node)
        return text.format(**fmt) if fmt else text
    except Exception:
        return key


def plural(n: int, one_key: str, other_key: str, /, **fmt) -> str:
    """Pick the singular vs plural key for a count and interpolate n.

    The selector args are positional-only so a catalog value may carry an
    ``{n}``/``{one_key}``/``{other_key}`` placeholder without colliding."""
    fmt.setdefault("n", n)
    return t(one_key if n == 1 else other_key, **fmt)


def app_available_locales() -> list:
    """Locale codes shipped under gui/messages/ (drives the selector)."""
    return sorted(p.name for p in MESSAGES_DIR.glob("[!._]*") if p.is_dir())
