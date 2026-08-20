"""Tests for spatialrisk.gee.vector_export — the geemap-free vector export.

The EE FeatureCollection is stubbed (Earth Engine is network-bound), but the
download itself goes through a real local HTTP server so the request/streaming
path is exercised for real.
"""

import io
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from spatialrisk.gee.vector_export import ee_export_vector


class FakeFeatureCollection:
    """Duck-typed stand-in for ee.FeatureCollection."""

    def __init__(self, url, property_names=("gaul0_name", "iso3_code")):
        """Record what the exporter asks for and hand back a canned URL."""
        self.url = url
        self.property_names = list(property_names)
        self.download_calls = []

    def getDownloadURL(self, filetype=None, selectors=None, filename=None):
        """Log the call and return the stub server's URL."""
        self.download_calls.append(
            {"filetype": filetype, "selectors": selectors, "filename": filename}
        )
        return self.url

    # selectors=None path: ee_object.first().propertyNames().getInfo()
    def first(self):
        """Stand in for the collection's first feature (returns self)."""
        return self

    def propertyNames(self):
        """Stand in for the property-name list object (returns self)."""
        return self

    def getInfo(self):
        """Resolve the stubbed property names client-side."""
        return list(self.property_names)


@pytest.fixture
def serve():
    """Start a one-payload HTTP server; yields a factory returning its URL."""
    servers = []

    def factory(payload, status=200):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}/download"

    yield factory
    for server in servers:
        server.shutdown()
        server.server_close()


def test_geojson_download_writes_payload_and_prepends_geo_selector(serve, tmp_path):
    """The payload lands on disk verbatim, and .geo leads the selector list."""
    payload = b'{"type": "FeatureCollection", "features": []}'
    fc = FakeFeatureCollection(serve(payload))
    out = tmp_path / "borders.geojson"

    ee_export_vector(fc, out, selectors=[], verbose=False)

    assert out.read_bytes() == payload
    assert fc.download_calls == [
        {"filetype": "geojson", "selectors": [".geo"], "filename": "borders"}
    ]


def test_selectors_default_to_collection_property_names(serve, tmp_path):
    """With no selectors the exporter asks the collection for its own."""
    fc = FakeFeatureCollection(serve(b"a,b\n1,2\n"), property_names=["a", "b"])

    ee_export_vector(fc, tmp_path / "table.csv", verbose=False)

    assert fc.download_calls[0]["selectors"] == ["a", "b"]


def test_shp_download_extracts_zip_and_removes_it(serve, tmp_path):
    """A shapefile arrives zipped: extract every member, drop the archive."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("aoi.shp", b"shp-bytes")
        z.writestr("aoi.dbf", b"dbf-bytes")
    fc = FakeFeatureCollection(serve(buffer.getvalue()))
    out = tmp_path / "aoi.shp"

    ee_export_vector(fc, out, selectors=["a"], verbose=False)

    assert (tmp_path / "aoi.shp").read_bytes() == b"shp-bytes"
    assert (tmp_path / "aoi.dbf").read_bytes() == b"dbf-bytes"
    assert not (tmp_path / "aoi.zip").exists()
    assert fc.download_calls[0]["filetype"] == "shp"


def test_shp_keep_zip_retains_archive(serve, tmp_path):
    """keep_zip leaves the downloaded archive in place."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("aoi.shp", b"shp-bytes")
    fc = FakeFeatureCollection(serve(buffer.getvalue()))

    ee_export_vector(
        fc, tmp_path / "aoi.shp", selectors=["a"], keep_zip=True, verbose=False
    )

    assert (tmp_path / "aoi.zip").exists()


def test_http_error_raises_with_status(serve, tmp_path):
    """A non-200 response raises with the status and writes no file."""
    fc = FakeFeatureCollection(serve(b"boom", status=400))
    out = tmp_path / "aoi.geojson"

    with pytest.raises(RuntimeError, match="400"):
        ee_export_vector(fc, out, selectors=[], verbose=False)
    assert not out.exists()


def test_unsupported_extension_raises(tmp_path):
    """Formats Earth Engine cannot emit are rejected before any request."""
    fc = FakeFeatureCollection("http://unused")

    with pytest.raises(ValueError, match="gpkg"):
        ee_export_vector(fc, tmp_path / "aoi.gpkg", selectors=[])
