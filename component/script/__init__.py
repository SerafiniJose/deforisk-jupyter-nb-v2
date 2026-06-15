"""Backward-compatibility shim: ``component.script`` -> ``spatialrisk``.

The package was renamed from ``component/script`` to ``spatialrisk`` (commit
05a5b08, a clean ``git mv`` that preserved every submodule path 1:1). The
notebooks and published docs still import from the old dotted path, e.g.::

    from component.script import Project, Dataset
    from component.script.gee.ee_fao_gaul import get_fao_gaul_features
    from component.script.variables import LocalRasterVar

This module installs a meta-path finder that transparently maps any
``component.script.<sub>`` import onto ``spatialrisk.<sub>`` (returning the *same*
module object, so identity checks and shared state hold), and re-exports the
top-level public API so ``from component.script import <name>`` keeps working.

This is a transitional aid. New code should import from ``spatialrisk`` directly.
"""

import importlib
import importlib.util
import sys
from importlib.abc import Loader, MetaPathFinder

_OLD = "component.script"
_NEW = "spatialrisk"


class _RedirectLoader(Loader):
    """Loader that returns an already-imported target module unchanged."""

    def __init__(self, module):
        self._module = module

    def create_module(self, spec):
        return self._module

    def exec_module(self, module):  # already executed by the real import
        pass


class _RedirectFinder(MetaPathFinder):
    """Map ``component.script.<x>`` imports onto ``spatialrisk.<x>``."""

    def find_spec(self, fullname, path=None, target=None):
        # Only intercept submodules of component.script -- never the package
        # itself (this very module) nor unrelated names.
        if not fullname.startswith(_OLD + "."):
            return None
        new_name = _NEW + fullname[len(_OLD):]
        try:
            module = importlib.import_module(new_name)
        except ModuleNotFoundError:
            # No spatialrisk equivalent (e.g. a name that never existed). Fall
            # through to the normal finders so the real ImportError surfaces.
            return None
        sys.modules[fullname] = module
        return importlib.util.spec_from_loader(fullname, _RedirectLoader(module))


def _install():
    if not any(isinstance(f, _RedirectFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _RedirectFinder())


_install()

# Re-export the top-level public API so ``from component.script import X`` works.
from spatialrisk import *  # noqa: E402,F401,F403
from spatialrisk import __all__ as _spatialrisk_all  # noqa: E402

__all__ = list(_spatialrisk_all)
