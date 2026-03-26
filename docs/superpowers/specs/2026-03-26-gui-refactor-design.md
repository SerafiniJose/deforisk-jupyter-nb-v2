# GUI Refactor: Flatten `component/` and Rename `model/` to `store/`

**Date:** 2026-03-26
**Scope:** `gui/` directory structure only — no logic changes.

## Problem

The `gui/component/` wrapper adds one unnecessary level of indirection. All imports read `gui.component.X.y` when `gui.X.y` is clearer and shorter. The `model/` sub-package name is ambiguous (sounds like an ML model); it holds reactive app state.

## Target Structure

```
gui/
├── solara_app.py
├── __init__.py
├── logging_config.toml
├── CLAUDE.md
├── store/              ← was component/model/
│   └── state_manager.py
├── scripts/            ← was component/scripts/
│   └── project_io.py
├── tile/               ← was component/tile/
│   ├── aoi_tile.py
│   ├── variables_tile.py
│   └── dataset_tile.py
└── widget/             ← was component/widget/
    ├── variable_modal.py
    └── variable_list.py
```

Empty stub packages (`component/parameter/`, `component/message/`) are deleted.

## Naming Rationale

- **`store/`** — follows Vue/Pinia/Redux convention; the project uses ipyvuetify (Vue-based), so "store" is the natural term for shared reactive state. `state_manager.py` holds `AppState` (a class with `solara.reactive()` fields) and the module-level `app_state` singleton.
- **`tile/`** — preserved: pysepal term for a full workflow-step panel. Each tile occupies one tab.
- **`widget/`** — preserved: reusable sub-components embedded inside tiles.
- **`scripts/`** — preserved: non-component utilities (load/save/list project I/O).

## Files with Import Changes

| File | Old import prefix | New import prefix |
|------|-------------------|-------------------|
| `gui/solara_app.py` | `gui.component.model.state_manager` | `gui.store.state_manager` |
| `gui/solara_app.py` | `gui.component.scripts.project_io` | `gui.scripts.project_io` |
| `gui/solara_app.py` | `gui.component.tile.*` | `gui.tile.*` |
| `gui/tile/variables_tile.py` | `gui.component.widget.*` | `gui.widget.*` |

`aoi_tile.py` and `dataset_tile.py` have no `gui.component.*` imports.

## Documentation

`gui/CLAUDE.md` component map and all path references updated to reflect the new structure.

## Out of Scope

- No logic, behaviour, or component API changes.
- No renaming of files (`state_manager.py`, `project_io.py`, etc.).
- No changes to `spatialrisk/` package.
