"""Importing a local raster registers it as a first-class Prediction so it shows
in the Inference outputs list and is selectable in Step 8 — Evaluation."""

import pytest

from gui.scripts.prediction_import import import_prediction


class _FakeFolders:
    def __init__(self, root):
        self.project_folder = root


class _FakeProject:
    """Minimal Project stand-in: just the bits import_prediction touches."""

    def __init__(self, root):
        self.folders = _FakeFolders(root)
        self.predictions = {}
        self.saves = 0

    def add_prediction(self, pred, key=None, auto_save=True):
        storage_key = key or pred.storage_key()
        pred.project = self
        self.predictions[storage_key] = pred
        if auto_save:
            self.save()

    def save(self):
        self.saves += 1


def _src_raster(tmp_path):
    src = tmp_path / "source" / "my map.tif"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"RASTERBYTES")
    return src


def test_import_copies_file_and_registers_prediction(tmp_path):
    proj = _FakeProject(tmp_path / "proj")
    src = _src_raster(tmp_path)

    pred = import_prediction(proj, str(src), name="my map", palette="stretch")

    # Copied into the project (portable), not referenced in place.
    assert pred.path.exists()
    assert pred.path.parent == (tmp_path / "proj" / "imported_predictions")
    assert pred.path.read_bytes() == b"RASTERBYTES"
    assert pred.path != src

    # Registered as a proper prediction with the user-typed name as its label.
    assert pred.name == "my map"
    assert pred.model_key == "my-map"            # sanitized for label/key
    assert pred.dataset_name == "imported"
    assert pred.display_palette == "stretch"
    assert pred.storage_key() in proj.predictions
    assert proj.saves >= 1


def test_import_disambiguates_duplicate_names(tmp_path):
    proj = _FakeProject(tmp_path / "proj")
    src = _src_raster(tmp_path)

    p1 = import_prediction(proj, str(src), name="map", palette="far")
    p2 = import_prediction(proj, str(src), name="map", palette="far")

    assert p1.storage_key() != p2.storage_key()   # two distinct registry entries
    assert p1.path != p2.path                      # two distinct files on disk
    assert len(proj.predictions) == 2


def test_import_missing_file_raises(tmp_path):
    proj = _FakeProject(tmp_path / "proj")
    with pytest.raises(FileNotFoundError):
        import_prediction(proj, str(tmp_path / "nope.tif"), name="x", palette="far")
