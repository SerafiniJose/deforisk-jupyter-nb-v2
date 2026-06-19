"""Map helpers for the Spatial Risk GUI.

Kept free of Solara/ipyvuetify so the map-interaction logic can be unit-tested
without a render harness.
"""


def is_mappable(var) -> bool:
    """True if a variable can be drawn on the map.

    A variable is mappable when it carries a fetched GEE image (``gee_images``)
    or is a local file-backed layer (``LocalRasterVar`` / ``LocalVectorVar``).
    Kept type-name based so this module stays free of the variable import chain.
    """
    if getattr(var, "gee_images", None):
        return True
    return type(var).__name__ in ("LocalRasterVar", "LocalVectorVar")


def add_vector_on_map(map_, path, name: str, key: str, style: dict = None):
    """Draw a local vector file on ``map_`` as a GeoJSON outline overlay.

    Reprojects to WGS84 (required by ipyleaflet) and replaces any existing layer
    registered under ``key`` so re-toggling doesn't stack duplicates.

    Args:
        map_: SepalMap instance.
        path: Path to a vector file readable by geopandas (shp/geojson/gpkg/…).
        name: Display name for the layer.
        key: Unequivocal map-layer key (used for later removal).
        style: ipyleaflet GeoJSON style; defaults to a black, unfilled outline.
    """
    import geopandas as gpd
    from pysepal.mapping import get_ipygeojson

    gdf = gpd.read_file(path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    style = style or {"color": "#000000", "weight": 2, "fillOpacity": 0.0}

    map_.remove_layer(key, none_ok=True)
    layer = get_ipygeojson(gdf, name, style)
    map_.add_layer(layer, key=key)
    return layer


def zoom_map_to_aoi(map_, aoi) -> bool:
    """Zoom ``map_`` to ``aoi`` using pysepal's built-in SepalMap zoom methods.

    Mirrors ``AoiView``'s own zoom logic so a loaded project frames its AOI the
    same way a freshly selected one does:

    * vector AOIs (DRAW / admin-with-geometry) → ``zoom_bounds`` on the
      GeoDataFrame's WGS84 ``total_bounds``;
    * GEE-only AOIs → ``zoom_ee_object`` on the feature collection.

    Args:
        map_: SepalMap instance (or None).
        aoi: pysepal ``AoiResult`` (or None).

    Returns:
        True if a zoom was performed, False if there was nothing to zoom to
        (no map, no AOI, or an AOI carrying neither geometry nor a feature
        collection — e.g. a non-GEE admin selection whose geometry was not
        fetched).
    """
    if map_ is None or aoi is None:
        return False

    gdf = getattr(aoi, "gdf", None)
    if gdf is not None:
        map_.zoom_bounds(gdf.total_bounds)
        return True

    feature_collection = getattr(aoi, "feature_collection", None)
    if feature_collection is not None:
        map_.zoom_ee_object(feature_collection)
        return True

    return False


def draw_aoi_on_map(map_, aoi, key: str = "aoi", style: dict = None) -> bool:
    """Draw a vector ``aoi``'s geometry on ``map_`` as a GeoJSON layer.

    Mirrors ``AoiView``'s own vector rendering so a restored AOI looks the same
    as a freshly selected one. Replaces any existing layer under ``key`` so
    re-running on reload doesn't stack duplicates. No-op (returns False) when
    there is no map or no geometry to draw (e.g. a GEE-lazy admin/asset AOI).

    Returns True if a layer was drawn, False otherwise.
    """
    if map_ is None or aoi is None:
        return False

    gdf = getattr(aoi, "gdf", None)
    if gdf is None:
        return False

    # Imported lazily so this module (and zoom_map_to_aoi's tests) stay free of
    # the pysepal/ipyleaflet import chain.
    from pysepal.mapping import get_ipygeojson

    try:
        map_.remove_layer(key, none_ok=True)
    except Exception:
        # Layer may not exist yet, or the map API may differ — drawing proceeds.
        pass

    name = getattr(aoi, "name", None) or key
    layer = get_ipygeojson(gdf, name, style)
    map_.add_layer(layer, key=key)
    return True


def show_aoi_on_map(map_, aoi) -> bool:
    """Draw ``aoi`` and zoom the map to it. Returns True if anything happened."""
    drew = draw_aoi_on_map(map_, aoi)
    zoomed = zoom_map_to_aoi(map_, aoi)
    return drew or zoomed
