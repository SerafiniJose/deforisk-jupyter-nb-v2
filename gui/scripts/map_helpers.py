"""Map helpers for the Spatial Risk GUI.

Kept free of Solara/ipyvuetify so the map-interaction logic can be unit-tested
without a render harness.
"""


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
