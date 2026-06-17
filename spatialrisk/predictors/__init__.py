"""Stateless prediction collaborators (offload seams).

These are pure / self-contained functions and classes extracted from the
former ``BaseRiskModel.apply()`` family. They never import ``spatialrisk``
at package-import time, so they stay importable while the top-level package
__init__ is mid-migration.
"""

from spatialrisk.predictors.supervised import SupervisedPredictor
from spatialrisk.predictors.blocks import supervised_block_fn, icar_block_fn
from spatialrisk.predictors.mw import MWPredictor
from spatialrisk.predictors.jnr import JNRPredictor
from spatialrisk.predictors.registration import (
    build_dataset_snapshot,
    make_prediction_payload,
    register_supervised,
)

__all__ = [
    "SupervisedPredictor",
    "MWPredictor",
    "JNRPredictor",
    "supervised_block_fn",
    "icar_block_fn",
    "register_supervised",
    "make_prediction_payload",
    "build_dataset_snapshot",
]
