"""Suite-wide collection-time setup.

Reacton's first ``t()`` call *during* a first render corrupts its internal
widget map and surfaces later as a ``use_event`` KeyError. It only reproduces
in isolated single-test runs, which makes it an unpleasant intermittent rather
than an honest failure.

Seven test modules already guard against this with an import-time
``t("common.cancel")`` plus a trail of ``# noqa: E402`` on the component
imports that must follow it. Five render modules do not
(test_evaluation, test_echarts_adapter, test_pipeline_header,
test_summary_tile_reactivity, test_postprocess_tile_threading). None of them
trips the bug today; doing the warm-up once here makes that a property of
the suite rather than a coincidence of which components each module happens
to mount.

This must stay an import-time call rather than an autouse fixture: conftest is
imported before any test module, whereas fixtures run only after the test
module — and its component imports — have already been evaluated.
"""

from gui.i18n import t

t("common.cancel")
