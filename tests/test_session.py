from spatialrisk.document import ProjectDocument
from spatialrisk.session import ProjectSession


def _doc(name="proj_d"):
    return ProjectDocument(project_name=name)


def test_from_document_exposes_snapshot_and_version():
    doc = _doc()
    session = ProjectSession.from_document(doc)

    # snapshot() returns the exact inert document back
    assert isinstance(session.snapshot(), ProjectDocument)
    assert session.snapshot().project_name == "proj_d"
    # a fresh session starts at doc_version 0
    assert session.doc_version == 0
    # convenience name accessor mirrors the document
    assert session.project_name == "proj_d"


def test_replace_creates_new_doc_bumps_version_and_freezes_prior_snapshot():
    session = ProjectSession.from_document(_doc("orig"))
    before = session.snapshot()
    before_version = session.doc_version

    returned = session._replace(project_name="renamed")

    # mutation produced a NEW document object, not in-place edit
    assert session.snapshot() is not before
    assert returned is session.snapshot()
    assert session.snapshot().project_name == "renamed"
    # the prior snapshot is unchanged (frozen, references-only)
    assert before.project_name == "orig"
    # version advanced exactly once
    assert session.doc_version == before_version + 1


def test_session_never_uses_model_copy_update_for_doc_state():
    # Regression guard (spec §13): Document state must go through validated
    # _replace, never model_copy(update=...), which skips validation.
    import inspect
    import spatialrisk.session as session_mod

    src = inspect.getsource(session_mod)
    assert "model_copy(update" not in src
    assert ".model_copy(" not in src


import pytest
from pydantic import ValidationError


def test_replace_rejects_non_json_nested_value():
    session = ProjectSession.from_document(_doc())
    before_version = session.doc_version

    class _NotJson:
        pass

    # a non-JSON object smuggled into the AOI GeoJSON map must be rejected
    # by re-validation (GeoJSONGeometry == dict[str, JsonValue]).
    with pytest.raises(ValidationError):
        session._replace(aoi={"bad": _NotJson()})

    # failed mutation leaves the document and version untouched
    assert session.snapshot().aoi is None
    assert session.doc_version == before_version


from spatialrisk.document import LocalRasterSpec, LocalVectorSpec, GEESpec, CatalogueRecipe
from spatialrisk.variables.models import RasterType, RasterizationMethod, DataType


def test_add_local_raster_registers_under_storage_key_and_bumps_version():
    session = ProjectSession.from_document(_doc())
    v0 = session.doc_version
    spec = LocalRasterSpec(
        kind="local_raster", name="forest", year=2020,
        path="/data/forest_2020.tif", raster_type=RasterType.categorical,
    )
    session.add_local_raster(spec)

    raw = session.snapshot().raw_variables
    assert "forest_2020" in raw                 # name_year storage key
    assert raw["forest_2020"].path == "/data/forest_2020.tif"
    assert session.doc_version == v0 + 1


def test_add_local_vector_static_uses_bare_name_key():
    session = ProjectSession.from_document(_doc())
    spec = LocalVectorSpec(
        kind="local_vector", name="roads",
        path="/data/roads.shp", rasterization_method=RasterizationMethod.binary,
    )
    session.add_local_vector(spec)
    assert "roads" in session.snapshot().raw_variables   # no year -> bare name


def test_add_gee_variable_registers_in_raw():
    session = ProjectSession.from_document(_doc())
    spec = GEESpec(
        kind="gee", name="altitude", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(
            source="catalogue", catalogue_key="altitude", export_kind="raster",
        ),
    )
    session.add_gee_variable(spec)
    assert "altitude" in session.snapshot().raw_variables
    assert session.snapshot().raw_variables["altitude"].kind == "gee"


from spatialrisk.document import VariableId


def test_set_aoi_stores_geojson_and_bumps_version():
    session = ProjectSession.from_document(_doc())
    v0 = session.doc_version
    geom = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    session.set_aoi(geom)
    assert session.snapshot().aoi == geom
    assert session.doc_version == v0 + 1


def test_set_base_raster_stores_qualified_ref():
    session = ProjectSession.from_document(_doc())
    spec = LocalRasterSpec(
        kind="local_raster", name="dem", path="/data/dem.tif",
        raster_type=RasterType.continuous,
    )
    session.add_local_raster(spec)
    ref = VariableId(source="raw", name="dem")
    session.set_base_raster(ref)
    assert session.snapshot().base_raster_ref == ref
    assert session.snapshot().base_raster_ref.source == "raw"
