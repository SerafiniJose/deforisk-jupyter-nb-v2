"""Route the PMTiles vector-tile server through SEPAL's jupyter-server-proxy.

Both tile servers bind 127.0.0.1 inside the kernel, so the browser needs a
route that reaches them. SEPAL sets ``LOCALTILESERVER_CLIENT_PREFIX`` to a
generic jupyter-server-proxy template (``…/proxy/{port}``) that forwards any
port in the sandbox — so it carries PMTiles as well as raster tiles. But
``vectortileserver`` never autodetects a prefix (its default is the raw
loopback URL plus the comm bridge, which doesn't mount under Solara), so the
prefix must be copied across before any ``TileClient`` is built. Only the
generic ``/proxy/{port}`` template form is borrowed: a route namespaced to one
concrete server would not forward the vector port. Pattern proven on SEPAL by
sepal-contrib/sbae-design.
"""

import os


def borrow_localtileserver_prefix(environ=os.environ):
    """Copy SEPAL's generic proxy prefix onto ``VECTORTILESERVER_CLIENT_PREFIX``.

    No-op when the raster prefix is absent or port-specific, or when a vector
    prefix is already set explicitly. Call at app startup, before any
    ``vectortileserver.TileClient`` is constructed.
    """
    raster_prefix = environ.get("LOCALTILESERVER_CLIENT_PREFIX")
    if raster_prefix and "/proxy/{port}" in raster_prefix:
        environ.setdefault("VECTORTILESERVER_CLIENT_PREFIX", raster_prefix)
