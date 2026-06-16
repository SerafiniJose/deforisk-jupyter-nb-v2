"""Make the repository root importable for the test suite.

``gui`` is an application package that is not part of the installed
distribution (only ``spatialrisk`` is installed, via an editable ``.pth``).
Under the bare ``pytest`` console script, ``sys.path[0]`` is the test
directory rather than the repo root, so ``import gui...`` would raise
``ModuleNotFoundError``. Inserting the repo root here fixes that without
changing how the editable-installed ``spatialrisk`` package resolves.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
