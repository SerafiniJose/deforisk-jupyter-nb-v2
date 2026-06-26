"""Draw sample sets on the map as PMTiles vector tiles.

Scales to millions of points: the browser fetches only the tiles for the current
viewport/zoom. Solara-free (mirrors prediction_map.py). Reachability on SEPAL
reuses the jupyter-loopback comm bridge that localtileserver already relies on —
it forwards Range requests and relays 206 responses over the kernel comm channel,
which is what pmtiles.js needs.
"""
import logging

logger = logging.getLogger("spatial_risk")

# Must match the tippecanoe -l layer name in spatialrisk/pmtiles_convert.py.
_SOURCE_LAYER = "points"


def build_sample_circle_style(url, *, strata_field="strata",
                              event_color="#d62728", forest_color="#2ca02c"):
    """MapLibre style: one circle layer colored by the strata attribute.

    event (strata == 1) -> red, forest (everything else) -> green.
    """
    match = ["match", ["get", strata_field], 1, event_color, forest_color]
    return {
        "version": 8,
        "sources": {"sample": {"type": "vector", "url": url}},
        "layers": [{
            "id": "sample-points",
            "type": "circle",
            "source": "sample",
            "source-layer": _SOURCE_LAYER,
            "paint": {
                "circle-radius": 4,
                "circle-opacity": 0.7,
                "circle-stroke-width": 1,
                "circle-color": match,
                "circle-stroke-color": match,
            },
        }],
    }


def _enable_loopback(port):
    """Route the tile server's localhost port through the jupyter-loopback bridge.

    Best-effort: on non-SEPAL frontends (or if the bridge is missing) this is a
    harmless no-op and the layer still works where the origin is shared.
    """
    if port is None:
        return
    try:
        import jupyter_loopback
        if not jupyter_loopback.is_comm_bridge_enabled():
            jupyter_loopback.enable_comm_bridge()
        jupyter_loopback.intercept_localhost(int(port))
    except Exception:
        logger.debug("jupyter-loopback unavailable for port %s; PMTiles tiles "
                     "may not reach the browser on SEPAL", port, exc_info=True)


def add_sample_pmtiles_on_map(map_, pmtiles_path, name, key):
    """Add a sample's PMTiles as one circle layer styled by strata.

    Replaces any layer already registered under ``key``. We construct the
    ipyleaflet ``PMTilesLayer`` directly (rather than
    ``client.create_leaflet_layer``) — Phase 0 confirmed the latter imports the
    undeclared ``mapbox_vector_tile`` package, while direct construction works
    with only ``pyvectortiles`` + ``pmtiles`` installed. ``TileClient`` is used
    purely for its range-capable tile server + ``pmtiles_url``.
    """
    from ipyleaflet import PMTilesLayer
    from pyvectortiles.client import TileClient

    client = TileClient(str(pmtiles_path))
    _enable_loopback(getattr(client, "port", None))   # Phase 0: attr is .port
    style = build_sample_circle_style(client.pmtiles_url)
    layer = PMTilesLayer(url=client.pmtiles_url, style=style)
    try:
        layer.name = name
    except Exception:
        pass
    map_.remove_layer(key, none_ok=True)
    map_.add_layer(layer, key=key)
    return layer


def remove_sample_pmtiles_from_map(map_, key):
    """Remove the PMTiles layer registered under ``key``."""
    map_.remove_layer(key, none_ok=True)
