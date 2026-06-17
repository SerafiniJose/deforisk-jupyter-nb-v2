"""Stateless prediction collaborators (offload seams).

These are pure / self-contained functions and classes extracted from the
former ``BaseRiskModel.apply()`` family. They never import ``spatialrisk``
at package-import time, so they stay importable while the top-level package
__init__ is mid-migration.
"""

from spatialrisk.predictors.blocks import icar_block_fn, supervised_block_fn
from spatialrisk.predictors.jnr import JNRPredictor
from spatialrisk.predictors.mw import MWPredictor
from spatialrisk.predictors.registration import (
    build_dataset_snapshot,
    make_prediction_payload,
    register_supervised,
)
from spatialrisk.predictors.supervised import SupervisedPredictor

__all__ = [
    "JNRPredictor",
    "MWPredictor",
    "SupervisedPredictor",
    "build_dataset_snapshot",
    "icar_block_fn",
    "make_prediction_payload",
    "register_supervised",
    "supervised_block_fn",
]
