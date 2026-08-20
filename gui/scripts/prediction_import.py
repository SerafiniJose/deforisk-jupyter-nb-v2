"""Import a user-supplied raster as a first-class :class:`Prediction`.

Lets the analyst bring a prediction map produced outside the app (e.g. exported
from QGIS or another pipeline) into Step 7 — Inference, so it renders on the map
and can be scored in Step 8 — Evaluation alongside computed predictions.

Solara-free (architecture contract #7): a pure adapter over the Project document,
called from the Inference tile. The heavy geo deps are not needed here — only a
file copy and registry write.
"""

import re
import shutil
from pathlib import Path
from typing import Any

IMPORT_DIR_NAME = "imported_predictions"
IMPORT_DATASET_NAME = "imported"


def sanitize_import_name(name: str) -> str:
    """Filesystem- and label-safe token from a free-text name.

    Spaces collapse to hyphens; characters outside ``[A-Za-z0-9._-]`` are dropped.
    Avoids ``_`` runs that ``evaluation.label_for`` would split on. Falls back to
    ``"imported"`` if nothing survives.
    """
    token = re.sub(r"\s+", "-", name.strip())
    token = re.sub(r"[^A-Za-z0-9._-]", "", token)
    return token or IMPORT_DATASET_NAME


def resolve_import_key(project: Any, name: str, src_suffix: str = "") -> str:
    """The model_key ``import_prediction`` would assign to *name* right now.

    Pure read — mirrors the import's duplicate disambiguation (suffix ``-2``,
    ``-3``, … while the registry key or the destination file is taken) so the
    GUI can preview the key before the copy happens.
    """
    dest_dir = Path(project.folders.project_folder) / IMPORT_DIR_NAME
    base = sanitize_import_name(name)
    model_key = base
    suffix = 2
    while (
        f"{model_key}__{IMPORT_DATASET_NAME}" in getattr(project, "predictions", {})
        or (dest_dir / f"{model_key}{src_suffix}").exists()
    ):
        model_key = f"{base}-{suffix}"
        suffix += 1
    return model_key


def import_prediction(
    project: Any,
    src_path: str,
    name: str,
    palette: str = "far",
    auto_save: bool = True,
):
    """Copy *src_path* into the project and register it as a Prediction.

    Parameters
    ----------
    project : Project
        Active project; the raster is copied under
        ``project.folders.project_folder / "imported_predictions"``.
    src_path : str
        Path to the local raster to import.
    name : str
        User-typed display name. Used (sanitized) as the prediction's
        ``model_key`` so it labels the outputs list and the Evaluation table.
    palette : str
        Map display palette: ``"far"`` (pinned 1..65535) or ``"stretch"``
        (auto-stretched to the file's range). Stored on the prediction.
    auto_save : bool
        Persist the project JSON after registering (default True).

    Returns
    -------
    Prediction
        The registered prediction.
    """
    from spatialrisk.predictions.prediction import Prediction

    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f"Raster to import not found: {src}")

    dest_dir = Path(project.folders.project_folder) / IMPORT_DIR_NAME
    dest_dir.mkdir(parents=True, exist_ok=True)

    model_key = resolve_import_key(project, name, src.suffix)

    dest = dest_dir / f"{model_key}{src.suffix}"
    shutil.copy2(src, dest)

    pred = Prediction(
        name=name,
        path=dest,
        model_key=model_key,
        dataset_name=IMPORT_DATASET_NAME,
        display_palette=palette,
    )
    pred.add_to_project(project, auto_save=auto_save)
    return pred
