"""The riskmapjnr wrappers must absolutise every path they forward.

``spatialrisk.rmj.vulnerability_map`` and ``spatialrisk.rmj.compute_dist_bins``
are thin wrappers over ``riskmapjnr.benchmark.*``. riskmapjnr resolves every path
it is given against the *process* CWD -- its own output defaults are bare
filenames (``"vulnerability_map.tif"``, ``"riskmap.tif"``,
``"defrate_per_cat.csv"``) -- and on SEPAL that CWD is the read-only shared
module mount, where creating a file dies with::

    [Errno 30] Read-only file system

Today every caller happens to pass an absolute path, so these tests do not pin a
bug that is live; they pin the *boundary* property, so it survives the next
caller. The riskmapjnr seam is monkeypatched rather than run: a real
vulnerability map is a full-raster block loop, and what is under test is the
argument that crosses the boundary, not the raster that comes back.

Siblings: ``test_default_output_paths_cwd.py`` (bare-filename defaults in our own
public functions) and ``test_gdal_readonly_cwd.py`` (GDAL's scratch files) cover
the other halves of the same SEPAL bug class.
"""

from pathlib import Path

import pytest

riskmapjnr = pytest.importorskip("riskmapjnr")

from spatialrisk.rmj import compute_dist_bins, vulnerability_map  # noqa: E402

# What compute_dist_bins' inner call returns; irrelevant to the assertions, but
# it must travel back out of the wrapper untouched.
BINS = [30.0, 60.0, 120.0]


@pytest.fixture()
def relative_cwd(tmp_path, monkeypatch):
    """A writable CWD that is *not* where any argument is meant to point.

    Deliberately writable: an unwritable CWD would make a relative path fail
    loudly, which is the easy half. The dangerous half is the machine where the
    relative path quietly resolves somewhere plausible, and that is what this
    reproduces.
    """
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


@pytest.fixture()
def recorded_vulnerability_map(monkeypatch):
    """Capture the kwargs handed to ``riskmapjnr.benchmark.vulnerability_map``."""
    calls = {}

    def fake_vulnerability_map(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(
        riskmapjnr.benchmark, "vulnerability_map", fake_vulnerability_map
    )
    return calls


@pytest.fixture()
def recorded_compute_dist_bins(monkeypatch):
    """Capture the args handed to ``riskmapjnr.benchmark.compute_dist_bins``."""
    calls = {}

    def fake_compute_dist_bins(dist_file, dist_thresh):
        calls["dist_file"] = dist_file
        calls["dist_thresh"] = dist_thresh
        return BINS

    monkeypatch.setattr(
        riskmapjnr.benchmark, "compute_dist_bins", fake_compute_dist_bins
    )
    return calls


# Every path-shaped argument vulnerability_map forwards, paired with the
# riskmapjnr parameter it arrives under (the wrapper renames one of them).
VULNERABILITY_PATHS = {
    "forest_file": "forest.tif",
    "dist_file": "forest_edge.tif",
    "subj_file": "subj.tif",
    "output_file": "vulnerability.tif",
}


def test_vulnerability_map_absolutises_relative_paths(
    relative_cwd, recorded_vulnerability_map
):
    """Bare filenames in, absolute paths out -- for inputs and the output alike."""
    vulnerability_map(
        forest_file="forest.tif",
        forest_edge_file="forest_edge.tif",
        dist_bins=BINS,
        subj_file="subj.tif",
        output_file="vulnerability.tif",
    )

    for param, name in VULNERABILITY_PATHS.items():
        forwarded = Path(recorded_vulnerability_map[param])
        assert forwarded.is_absolute(), f"{param} crossed the boundary relative"
        # .resolve() on the expectation too: the wrapper normalises symlinks,
        # and a tmp_path under a symlinked /tmp would otherwise fail spuriously.
        assert forwarded == (relative_cwd / name).resolve()

    # Non-path arguments must be forwarded untouched.
    assert recorded_vulnerability_map["dist_bins"] == BINS


def test_vulnerability_map_leaves_absolute_paths_alone(
    tmp_path, relative_cwd, recorded_vulnerability_map
):
    """The normal case -- absolute in, the same absolute out, not re-rooted."""
    out = tmp_path / "outputs" / "vulnerability.tif"

    vulnerability_map(
        forest_file=tmp_path / "forest.tif",
        forest_edge_file=tmp_path / "forest_edge.tif",
        dist_bins=BINS,
        subj_file=tmp_path / "subj.tif",
        output_file=out,
    )

    assert Path(recorded_vulnerability_map["output_file"]) == out.resolve()
    assert relative_cwd not in Path(recorded_vulnerability_map["output_file"]).parents


def test_vulnerability_map_forwards_str_paths(relative_cwd, recorded_vulnerability_map):
    """Paths cross as str, not Path: riskmapjnr hands them to GDAL verbatim."""
    vulnerability_map(
        forest_file="forest.tif",
        forest_edge_file="forest_edge.tif",
        dist_bins=BINS,
        subj_file="subj.tif",
        output_file="vulnerability.tif",
    )

    for param in VULNERABILITY_PATHS:
        assert isinstance(recorded_vulnerability_map[param], str)


def test_compute_dist_bins_absolutises_a_relative_path(
    relative_cwd, recorded_compute_dist_bins
):
    """The one path this wrapper forwards is absolute at the boundary."""
    bins = compute_dist_bins(forest_edge_file="forest_edge.tif", dist_thresh=1200.0)

    forwarded = Path(recorded_compute_dist_bins["dist_file"])
    assert forwarded.is_absolute()
    assert forwarded == (relative_cwd / "forest_edge.tif").resolve()
    assert isinstance(recorded_compute_dist_bins["dist_file"], str)
    # Resolving the path must not disturb what comes back.
    assert recorded_compute_dist_bins["dist_thresh"] == 1200.0
    assert bins == BINS


def test_compute_dist_bins_leaves_an_absolute_path_alone(
    tmp_path, relative_cwd, recorded_compute_dist_bins
):
    """Absolute in, the same absolute out."""
    edge = tmp_path / "data" / "forest_edge.tif"

    compute_dist_bins(forest_edge_file=edge, dist_thresh=1200.0)

    assert Path(recorded_compute_dist_bins["dist_file"]) == edge.resolve()
