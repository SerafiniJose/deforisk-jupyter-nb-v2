from spatialrisk.document import ProjectDocument
from spatialrisk.session import ProjectSession, FolderResolver


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


from spatialrisk.document import VariableId, DatasetSpec, GLMSpec, PredictionSpec


def test_register_dataset_model_and_prediction():
    session = ProjectSession.from_document(_doc())

    ds = DatasetSpec(
        name="calib",
        target_ref=VariableId(source="processed", name="defor", year=2020),
        feature_refs=(VariableId(source="processed", name="dem"),),
    )
    session.register_dataset(ds)
    assert "calib" in session.snapshot().datasets

    model = GLMSpec(
        name="glm1", model_type="glm", project_name="proj_d",
        dataset_name="calib", target_name="defor", feature_names=("dem",),
        year=2020, formula="defor ~ scale(dem)", parameters={},
        sampling=None, samples_path=None, trained=False, trained_at=None,
        n_samples=None, deviance=None, estimator_pickle=None,
    )
    session.register_model(model, key="glm_glm1")
    assert "glm_glm1" in session.snapshot().models

    pred = PredictionSpec(
        path="/data/pred_2020.tif", model_key="glm_glm1", dataset_name="calib",
        year=2020,
    )
    session.register_prediction(pred, key="glm_glm1_2020")
    assert "glm_glm1_2020" in session.snapshot().predictions
    assert session.snapshot().predictions["glm_glm1_2020"].path == "/data/pred_2020.tif"


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


def _session_with_temporal_defor():
    session = ProjectSession.from_document(_doc())
    for yr in (2015, 2020):
        session.add_local_raster(LocalRasterSpec(
            kind="local_raster", name="defor", year=yr,
            path=f"/data/defor_{yr}.tif", raster_type=RasterType.categorical,
        ))
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path="/data/dem.tif",
        raster_type=RasterType.continuous,
    ))
    return session


def test_get_variable_and_instances_and_temporal():
    session = _session_with_temporal_defor()

    # storage-key lookup (name_year)
    v = session.get_variable("defor", year=2020, source="raw")
    assert v is not None and v.path == "/data/defor_2020.tif"
    # static lookup by bare name
    assert session.get_variable("dem", source="raw").path == "/data/dem.tif"
    # missing -> None
    assert session.get_variable("defor", year=1999, source="raw") is None

    insts = session.get_all_instances("defor", source="raw")
    assert {i.year for i in insts} == {2015, 2020}

    assert session.is_temporal("defor", source="raw") is True
    assert session.is_temporal("dem", source="raw") is False
    assert session.get_variable_years("defor", source="raw") == [2015, 2020]


def test_list_sources_vs_materialized_no_double_count():
    session = ProjectSession.from_document(_doc())
    # a GEE source recipe that has been materialized into a local raster
    session.add_gee_variable(GEESpec(
        kind="gee", name="altitude", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(
            source="catalogue", catalogue_key="altitude", export_kind="raster",
        ),
        materialized_key="altitude_product",
    ), key="altitude_src")
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="altitude",
        path="/data/altitude.tif", raster_type=RasterType.continuous,
        derived_from="altitude_src",
    ), key="altitude_product")

    # list_sources: only the recipe(s)
    sources = session.list_sources(source="raw")
    assert sources == ["altitude_src"]

    # list_materialized: only the on-disk product, NOT the GEE source whose
    # materialized_key is set
    materialized = session.list_materialized(source="raw")
    assert materialized == ["altitude_product"]

    # query honors provenance: get_all_instances("altitude") returns the
    # product only (the materialized GEE source is skipped)
    insts = session.get_all_instances("altitude", source="raw")
    assert len(insts) == 1
    assert insts[0].kind == "local_raster"


def _session_for_filters():
    session = ProjectSession.from_document(_doc())
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path="/data/dem.tif",
        raster_type=RasterType.continuous, tags=("elevation", "terrain"),
        year=2020,
    ))
    session.add_local_vector(LocalVectorSpec(
        kind="local_vector", name="roads", path="/data/roads.shp",
        rasterization_method=RasterizationMethod.binary, tags=("infrastructure",),
    ))
    return session


def test_list_variables_and_filters():
    session = _session_for_filters()

    # no filter -> all raw
    assert set(session.list_variables(source="raw")) == {"dem_2020", "roads"}

    # filter by computed data_type property (works across all kinds)
    rasters = session.list_variables(source="raw", data_type=DataType.raster)
    assert set(rasters) == {"dem_2020"}

    # filter by year (scalar)
    assert set(session.filter_by_attrs(source="raw", year=2020)) == {"dem_2020"}

    # filter by tag (OR semantics)
    tagged = session.filter_by_tags("elevation", look_up_in="raw")
    assert set(tagged) == {"dem_2020"}
    assert set(session.filter_by_tags(["infrastructure", "x"], look_up_in="raw")) == {"roads"}


from box import Box


def test_folder_resolver_preserves_it_name_suffix(tmp_path):
    resolver = FolderResolver(project_name="proj_d", data_root=tmp_path)

    plain = resolver.folders()
    assert isinstance(plain, Box)
    # suffix-free keys: no iteration prefix
    assert plain.glm_model.name == "far_glm"
    assert plain.project_folder == tmp_path / "proj_d"

    suffixed = resolver.folders(it_name="run1")
    # iteration suffix prepended to the suffixed folders
    assert suffixed.glm_model.name == "run1_far_glm"
    assert suffixed.icar_model.name == "run1_far_icar"
    assert suffixed.rf_model.name == "run1_far_rf"
    assert suffixed.rmj_bm.name == "run1_rmj_bm"
    # non-suffixed folders are unchanged
    assert suffixed.processed_data_folder.name == "data"


def test_session_exposes_folders_callable(tmp_path):
    # FolderResolver's iteration-suffix mechanic; ProjectSession.folders() delegates
    # to it. session.folders() as a method is the single contract (see the
    # geoprocessing/handle tests for the session-level call).
    box = FolderResolver(project_name="fp", data_root=tmp_path).folders(it_name="r1")
    assert box.glm_model.name == "r1_far_glm"


class FakeStore:
    """In-memory ProjectStorePort double."""
    def __init__(self):
        self.docs = {}
    def save(self, doc):
        self.docs[doc.project_name] = doc
        return f"mem://{doc.project_name}"
    def load(self, name):
        return self.docs[name]
    def list(self):
        return sorted(self.docs)
    def exists(self, name):
        return name in self.docs


def test_create_save_open_roundtrip_via_store():
    store = FakeStore()

    session = ProjectSession.create("newproj", store=store, gee=None)
    assert session.snapshot().project_name == "newproj"
    assert session.store is store

    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path="/data/dem.tif",
        raster_type=RasterType.continuous,
    ))
    ref = session.save()
    assert ref == "mem://newproj"
    assert store.exists("newproj")

    reopened = ProjectSession.open("newproj", store=store, gee=None)
    assert "dem" in reopened.snapshot().raw_variables
    assert reopened.doc_version == 0   # freshly opened doc starts at version 0


def test_variable_handle_resolves_spec_and_base_ref_disambiguates_source():
    session = ProjectSession.from_document(_doc())
    # same name "dem" in BOTH raw and processed
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path="/raw/dem.tif",
        raster_type=RasterType.continuous,
    ))
    # add a processed dem with a different path
    new_proc = dict(session.snapshot().processed_variables)
    new_proc["dem"] = LocalRasterSpec(
        kind="local_raster", name="dem", path="/proc/dem.tif",
        raster_type=RasterType.continuous,
    )
    session._replace(processed_variables=new_proc)

    # base_raster_ref pinned to the PROCESSED source resolves unambiguously
    session.set_base_raster(VariableId(source="processed", name="dem"))
    base_handle = session.base_raster_handle()
    assert base_handle is not None
    assert base_handle.spec.path == "/proc/dem.tif"
    assert base_handle.key == "dem"
    assert base_handle.source == "processed"

    # a plain handle by (source, name)
    raw_handle = session.get_variable_handle("dem", source="raw")
    assert raw_handle.spec.path == "/raw/dem.tif"


import numpy as np
import rasterio
from rasterio.transform import from_origin


def _write_raster(path, arr, nodata=None):
    arr = np.asarray(arr)
    with rasterio.open(
        path, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1],
        count=1, dtype=arr.dtype, crs="EPSG:4326",
        transform=from_origin(0, arr.shape[0], 1, 1),
        nodata=nodata,
    ) as dst:
        dst.write(arr, 1)


def test_dataset_handle_resolves_paths_rastertype_and_levels(tmp_path):
    defor = tmp_path / "defor_2020.tif"
    subj = tmp_path / "subj.tif"
    dem = tmp_path / "dem.tif"
    _write_raster(defor, np.array([[0, 1], [1, 0]], dtype="int16"))
    _write_raster(subj, np.array([[1, 2], [3, 2]], dtype="int16"))   # categorical levels 1,2,3
    _write_raster(dem, np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"))

    session = ProjectSession.from_document(_doc())
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="defor", year=2020, path=str(defor),
        raster_type=RasterType.categorical,
    ))
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="subj", path=str(subj),
        raster_type=RasterType.categorical,
    ))
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path=str(dem),
        raster_type=RasterType.continuous,
    ))

    ds = DatasetSpec(
        name="calib", year=2020,
        target_ref=VariableId(source="raw", name="defor", year=2020),
        feature_refs=(
            VariableId(source="raw", name="dem"),
            VariableId(source="raw", name="subj"),
        ),
    )
    session.register_dataset(ds)

    resolved = session.get_dataset_handle("calib").resolve()

    assert resolved.target.path == str(defor)
    feats = {f.name: f for f in resolved.features}
    assert feats["dem"].raster_type == RasterType.continuous
    assert feats["dem"].levels is None              # continuous -> no levels
    assert feats["subj"].raster_type == RasterType.categorical
    assert feats["subj"].levels == [1, 2, 3]        # read from the real raster


def test_dataset_handle_temporal_target_requires_year(tmp_path):
    session = ProjectSession.from_document(_doc())
    for yr in (2015, 2020):
        session.add_local_raster(LocalRasterSpec(
            kind="local_raster", name="defor", year=yr,
            path=str(tmp_path / f"defor_{yr}.tif"), raster_type=RasterType.categorical,
        ))
    # target_ref omits the year for a temporal target -> resolve must raise
    ds = DatasetSpec(
        name="bad",
        target_ref=VariableId(source="raw", name="defor"),   # no year
    )
    session.register_dataset(ds)
    with pytest.raises(ValueError, match="multitemporal"):
        session.get_dataset_handle("bad").resolve()


def test_model_handle_delegates_fit_and_apply_to_predictor():
    session = ProjectSession.from_document(_doc("mh"))
    session.register_model(GLMSpec(
        name="glm1", model_type="glm", project_name="mh",
        dataset_name="calib", target_name="defor", feature_names=("dem",),
        year=2020, formula="defor ~ scale(dem)", parameters={},
        sampling=None, samples_path=None, trained=False, trained_at=None,
        n_samples=None, deviance=None, estimator_pickle=None,
    ), key="glm_glm1")

    calls = {}

    class FakePredictor:
        def fit(self, session, model_key, **kw):
            calls["fit"] = (model_key, kw)
            return "FITTED"
        def apply(self, session, model_key, out_path, mask=None, **kw):
            calls["apply"] = (model_key, out_path, mask)
            return out_path

    handle = session.get_model_handle("glm_glm1", predictor=FakePredictor())
    assert handle.spec.model_type == "glm"

    assert handle.fit(foo=1) == "FITTED"
    assert calls["fit"] == ("glm_glm1", {"foo": 1})

    assert handle.apply(out_path="/data/pred.tif", mask="/data/mask.tif") == "/data/pred.tif"
    assert calls["apply"] == ("glm_glm1", "/data/pred.tif", "/data/mask.tif")


def test_process_all_runs_materialize_then_reproject_then_rasterize(monkeypatch):
    session = ProjectSession.from_document(_doc("orch"))
    order = []
    monkeypatch.setattr(session, "materialize_all", lambda **kw: order.append("materialize"))
    monkeypatch.setattr(session, "reproject_and_match_all", lambda **kw: order.append("reproject"))
    monkeypatch.setattr(session, "rasterize_all", lambda **kw: order.append("rasterize"))

    session.process_all(source="raw")
    assert order == ["materialize", "reproject", "rasterize"]


def test_materialize_all_iterates_gee_sources(monkeypatch):
    session = ProjectSession.from_document(_doc("m"))
    session.add_gee_variable(GEESpec(
        kind="gee", name="altitude", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(
            source="catalogue", catalogue_key="altitude", export_kind="raster",
        ),
    ), key="altitude_src")
    # an already-materialized GEE source must be skipped
    session.add_gee_variable(GEESpec(
        kind="gee", name="slope", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(
            source="catalogue", catalogue_key="slope", export_kind="raster",
        ),
        materialized_key="slope_product",
    ), key="slope_src")

    seen = []
    session._materialize_one = lambda key, spec, **kw: seen.append(key)
    session.materialize_all(source="raw")
    assert seen == ["altitude_src"]   # slope_src skipped (already materialized)


def test_session_module_imports_no_ee():
    # The Session module must NOT import `ee` (that lives only in GEEAdapter).
    import inspect
    import spatialrisk.session as session_mod
    src = inspect.getsource(session_mod)
    assert "import ee" not in src
    assert "from ee" not in src


def test_full_session_suite_collects():
    # sanity: snapshot is the only crossing-boundary artifact and stays a
    # ProjectDocument after a sequence of mutations
    session = ProjectSession.create("final", store=None, gee=None)
    session.add_local_raster(LocalRasterSpec(
        kind="local_raster", name="dem", path="/d/dem.tif",
        raster_type=RasterType.continuous,
    ))
    session.set_base_raster(VariableId(source="raw", name="dem"))
    assert isinstance(session.snapshot(), ProjectDocument)
    assert session.doc_version == 2


def test_materialize_out_path_roots_in_raw_folder(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore
    store = LocalFSProjectStore(data_root=tmp_path)
    session = ProjectSession.from_document(_doc("paths"), store=store)
    session.add_gee_variable(GEESpec(
        kind="gee", name="altitude", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(source="catalogue", catalogue_key="altitude",
                               export_kind="raster"),
    ), key="altitude")
    out = session._materialize_out_path("altitude")
    raw = str(session.folders().data_raw_folder)
    assert out == f"{raw}/altitude.tif"


def test_materialize_out_path_vector_uses_shp_and_year(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore
    store = LocalFSProjectStore(data_root=tmp_path)
    session = ProjectSession.from_document(_doc("paths2"), store=store)
    session.add_gee_variable(GEESpec(
        kind="gee", name="aoi", year=2020, data_type=DataType.vector,
        recipe=CatalogueRecipe(source="catalogue", catalogue_key="aoi_fao_gaul",
                               params={"iso": "MTQ"}, export_kind="vector"),
    ), key="aoi_2020")
    out = session._materialize_out_path("aoi_2020")
    raw = str(session.folders().data_raw_folder)
    assert out == f"{raw}/aoi_2020.shp"


def test_materialize_spec_resolves_gee_var_from_raw(tmp_path):
    from spatialrisk.persistence import LocalFSProjectStore
    store = LocalFSProjectStore(data_root=tmp_path)
    session = ProjectSession.from_document(_doc("ms"), store=store)
    session.add_gee_variable(GEESpec(
        kind="gee", name="altitude", data_type=DataType.raster,
        raster_type=RasterType.continuous,
        recipe=CatalogueRecipe(source="catalogue", catalogue_key="altitude",
                               export_kind="raster"),
    ), key="altitude")
    spec = session.materialize_spec("altitude")
    assert spec.var_key == "altitude"
    assert spec.export_kind == "raster"
    assert spec.out_path.endswith("/altitude.tif")
    assert spec.recipe.catalogue_key == "altitude"
