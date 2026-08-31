"""Tests for borrowing SEPAL's jupyter-server-proxy prefix for vector tiles.

On SEPAL, ``LOCALTILESERVER_CLIENT_PREFIX`` holds a generic ``/proxy/{port}``
jupyter-server-proxy template that forwards any sandbox port — so it can carry
the PMTiles server too. ``vectortileserver`` never autodetects a prefix; it only
honors ``VECTORTILESERVER_CLIENT_PREFIX``. The helper copies the raster prefix
across (pattern proven by sepal-contrib/sbae-design).
"""

from gui.scripts.tile_proxy import borrow_localtileserver_prefix

GENERIC = "https://sepal.io/api/sandbox/jupyter/proxy/{port}"


def test_borrows_generic_proxy_prefix():
    """Generic /proxy/{port} raster prefix is copied to the vector env var."""
    env = {"LOCALTILESERVER_CLIENT_PREFIX": GENERIC}
    borrow_localtileserver_prefix(env)
    assert env["VECTORTILESERVER_CLIENT_PREFIX"] == GENERIC


def test_does_not_overwrite_explicit_vector_prefix():
    """An explicit vector prefix wins over the borrowed raster one."""
    env = {
        "LOCALTILESERVER_CLIENT_PREFIX": GENERIC,
        "VECTORTILESERVER_CLIENT_PREFIX": "/custom/{port}",
    }
    borrow_localtileserver_prefix(env)
    assert env["VECTORTILESERVER_CLIENT_PREFIX"] == "/custom/{port}"


def test_ignores_missing_raster_prefix():
    """No raster prefix means loopback stays the default."""
    env = {}
    borrow_localtileserver_prefix(env)
    assert "VECTORTILESERVER_CLIENT_PREFIX" not in env


def test_ignores_port_specific_prefix():
    """A port-specific raster route is not borrowed."""
    # A route namespaced to one concrete server/port would not forward the
    # vector server's port — leave loopback in place.
    env = {"LOCALTILESERVER_CLIENT_PREFIX": "/proxy/8888/localtileserver"}
    borrow_localtileserver_prefix(env)
    assert "VECTORTILESERVER_CLIENT_PREFIX" not in env
