"""delete_project() removes a saved project's folder — and refuses anything else.

``shutil.rmtree`` is irreversible and these folders reach several GB, so the
guards (not the happy path) are the point of this file.
"""

import json
from pathlib import Path

import pytest

from gui.scripts.project_io import delete_project, project_dir_size


def _write_project(data_dir: Path, name: str) -> Path:
    folder = data_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}_project.json").write_text(
        json.dumps({"project_name": name}), encoding="utf-8"
    )
    return folder


def test_deletes_the_whole_folder(tmp_path):
    folder = _write_project(tmp_path, "GUY")
    (folder / "data").mkdir()
    (folder / "data" / "big.tif").write_bytes(b"x" * 2048)

    assert delete_project("GUY", tmp_path) is True
    assert not folder.exists()


def test_missing_project_returns_false(tmp_path):
    assert delete_project("nope", tmp_path) is False


def test_deletes_a_corrupt_project(tmp_path):
    # An unreadable manifest is exactly the case a user most wants to clean up.
    folder = tmp_path / "broken"
    folder.mkdir()
    (folder / "broken_project.json").write_text("{not json", encoding="utf-8")

    assert delete_project("broken", tmp_path) is True
    assert not folder.exists()


def test_refuses_a_directory_that_is_not_a_project(tmp_path):
    (tmp_path / "random").mkdir()
    with pytest.raises(ValueError):
        delete_project("random", tmp_path)
    assert (tmp_path / "random").exists()


@pytest.mark.parametrize("name", ["", "   ", ".", "..", "../escape", "a/b", "a\\b"])
def test_refuses_traversal_and_separators(tmp_path, name):
    with pytest.raises(ValueError):
        delete_project(name, tmp_path)


def test_refuses_an_absolute_path(tmp_path):
    outside = _write_project(tmp_path, "outside")  # real, but addressed absolutely
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with pytest.raises(ValueError):
        delete_project(str(outside), data_dir)
    assert outside.exists()


def test_refuses_a_symlinked_project(tmp_path):
    # A symlink resolves outside data_dir, so the parent check rejects it — we do
    # not follow a link out of the data dir to rmtree someone's home.
    real = _write_project(tmp_path / "elsewhere", "real")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "real").symlink_to(real, target_is_directory=True)

    with pytest.raises(ValueError):
        delete_project("real", data_dir)
    assert real.exists()


def test_project_dir_size_sums_files(tmp_path):
    folder = _write_project(tmp_path, "sized")
    (folder / "a.tif").write_bytes(b"x" * 1000)
    (folder / "sub").mkdir()
    (folder / "sub" / "b.tif").write_bytes(b"x" * 2000)

    assert project_dir_size("sized", tmp_path) >= 3000  # rasters + the manifest


def test_project_dir_size_missing_is_zero(tmp_path):
    assert project_dir_size("nope", tmp_path) == 0


def test_delete_project_symlink_loop_raises_valueerror(tmp_path):
    """A symlink loop in .resolve() should surface as ValueError, not RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Create a symlink loop: loopy -> loopy
    loopy = data_dir / "loopy"
    loopy.symlink_to(data_dir / "loopy")

    with pytest.raises(ValueError):
        delete_project("loopy", data_dir)


def test_project_dir_size_symlink_loop_returns_zero(tmp_path):
    """A symlink loop should cause project_dir_size to return 0, not raise RuntimeError."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Create a symlink loop: loopy -> loopy
    loopy = data_dir / "loopy"
    loopy.symlink_to(data_dir / "loopy")

    assert project_dir_size("loopy", data_dir) == 0


def test_project_dir_size_unsafe_name_returns_zero(tmp_path):
    """An unsafe name in _project_dir should cause project_dir_size to return 0."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # The "../escape" case is rejected by the path component check in _project_dir,
    # which raises ValueError. project_dir_size should catch it and return 0.
    assert project_dir_size("../escape", data_dir) == 0
