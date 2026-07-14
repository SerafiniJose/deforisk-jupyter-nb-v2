"""delete_project() removes a saved project's folder — and refuses anything else.

``shutil.rmtree`` is irreversible and these folders reach several GB, so the
guards (not the happy path) are the point of this file.
"""

import json
import shutil
from pathlib import Path

import pytest

from gui.scripts.project_io import delete_project, list_project_infos, project_dir_size


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


def test_a_partial_delete_keeps_the_manifest_so_the_leftover_stays_deletable(
    tmp_path, monkeypatch
):
    """A subtree that refuses to go must not orphan a multi-GB folder.

    A plain rmtree(folder) unlinks in scandir order, so the manifest is routinely
    removed *before* a deep subdirectory — and if that subdirectory then fails, the
    leftover no longer looks like a project: list_project_infos skips it (it vanishes
    from the Manage list) and delete_project refuses it ("no manifest"). It would be
    invisible AND undeletable through the UI. So the manifest goes last: the failed
    project still lists, and a retry still finishes the job.
    """
    folder = _write_project(tmp_path, "GUY")
    (folder / "data").mkdir()
    (folder / "data" / "big.tif").write_bytes(b"x" * 2048)

    def boom(path, *args, **kwargs):
        raise OSError(f"Device or resource busy: {path}")

    monkeypatch.setattr(shutil, "rmtree", boom)
    with pytest.raises(OSError):
        delete_project("GUY", tmp_path)

    assert (folder / "GUY_project.json").exists()          # still a project …
    assert [i.name for i in list_project_infos(tmp_path)] == ["GUY"]  # … still listed

    monkeypatch.undo()  # the transient failure clears; the user hits Delete again
    assert delete_project("GUY", tmp_path) is True
    assert not folder.exists()


def test_a_late_arrival_after_the_child_pass_keeps_the_manifest_and_stays_listed(
    tmp_path, monkeypatch
):
    """"Manifest last" must hold even when the folder gains an entry AFTER the
    child-removal loop already took its one-time snapshot — a writer that slipped
    the mark, or an NFS silly-rename of a top-level file (e.g. aoi.geojson) still
    held open by a reader. The loop never sees a late arrival, so it never removes
    it. Unlinking the manifest anyway and letting rmdir() fail on the leftover
    would strip the manifest from a folder that is about to become exactly the
    invisible, undeletable orphan "manifest last" exists to prevent: gone from
    list_project_infos, and refused by delete_project itself ("no manifest").
    """
    folder = _write_project(tmp_path, "GUY")
    (folder / "aoi.geojson").write_bytes(b"{}")
    (folder / "data").mkdir()
    (folder / "data" / "big.tif").write_bytes(b"x" * 2048)

    real_rmtree = shutil.rmtree

    def sneaky_rmtree(path, *args, **kwargs):
        # A late arrival landing after the child loop's one-time snapshot was
        # taken — simulates a writer (or an NFS silly-rename) slipping a new
        # top-level file into the folder while this subtree is being removed.
        (folder / "late.tmp").write_bytes(b"x")
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shutil, "rmtree", sneaky_rmtree)

    with pytest.raises(OSError):
        delete_project("GUY", tmp_path)

    assert (folder / "GUY_project.json").exists()                    # still a project …
    assert [i.name for i in list_project_infos(tmp_path)] == ["GUY"]  # … still listed

    monkeypatch.undo()
    (folder / "late.tmp").unlink()  # the race resolves; the user hits Delete again
    assert delete_project("GUY", tmp_path) is True
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
