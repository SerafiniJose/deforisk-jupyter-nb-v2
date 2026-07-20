"""Guard test for the ipecharts dependency (Evaluation tile migration, Task 1).

ipecharts must be importable in the live env AND declared on both installation
paths — environment.yml (heavy local dev) and sepal_environment.yml (lean
deploy). This keeps the declaration honest: if either file drifts from the
installed version, this test catches it.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PINNED_SPEC = "ipecharts>=1.4,<2"


def test_ipecharts_is_importable():
    import ipecharts
    from ipecharts import EChartsWidget  # noqa: F401

    assert ipecharts.__version__


def test_ipecharts_pinned_in_environment_yml():
    text = (ROOT / "environment.yml").read_text()
    assert PINNED_SPEC in text


def test_ipecharts_pinned_in_sepal_environment_yml():
    text = (ROOT / "sepal_environment.yml").read_text()
    assert PINNED_SPEC in text
