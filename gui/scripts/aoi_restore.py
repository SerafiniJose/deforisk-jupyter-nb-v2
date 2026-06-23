"""Pure helpers for restoring an AOI selection in the GUI on project load.

Solara-free and EE-free so they unit-test without a render harness. pygaul is
imported lazily inside the functions.
"""

from typing import Dict, Optional


def admin_parent_chain(method: str, code: Optional[str]) -> Dict[int, str]:
    """Map a final GAUL admin code to its full ``{level: code}`` parent chain.

    Restoring an ADMIN1/ADMIN2 cascade needs the parent codes (to load the child
    dropdown lists), but only the final code is persisted. Look the parents up in
    pygaul's GAUL 2024 parquet. Returns ``{}`` when the code is falsy/unknown or
    pygaul is unavailable, so the caller can restore the method only.
    """
    if not code:
        return {}
    try:
        import pygaul

        level = int(method[-1])
        df = pygaul._df()
        rows = df[df[f"gaul{level}_code"].astype(str) == str(code)]
        if rows.empty:
            return {}
        row = rows.iloc[0]
        return {lvl: str(row[f"gaul{lvl}_code"]) for lvl in range(level + 1)}
    except Exception:
        return {}
