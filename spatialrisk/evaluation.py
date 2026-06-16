"""Quantitative model evaluation (udef-arp accuracy indices).

Promoted verbatim from notebooks/6.models_evaluation.ipynb so the GUI and the
notebook share one implementation. Native two-explicit-layer port of
forestatrisk.validation_udef_arp — no forestatrisk dependency.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd
from osgeo import gdal

FAMILY = {"glm": "GLM", "rf": "RF", "icar": "ICAR", "mw": "MW", "jnr": "JNR"}
FOREST_VAR = "forest_gfc"  # dataset feature used as 'forest at period start'


def interval_from_target(name):
    """'forest_loss_2015_2020' -> 5; None if fewer than two 4-digit years."""
    yrs = [int(y) for y in re.findall(r"\d{4}", name or "")]
    return (yrs[1] - yrs[0]) if len(yrs) >= 2 else None


def label_for(pred):
    """Short display label for a prediction (e.g. 'GLM', 'MW_w11')."""
    fam = FAMILY.get(pred.model_key.split("_")[0], pred.model_key)
    return f"{fam}_w{pred.window}" if pred.window is not None else fam
