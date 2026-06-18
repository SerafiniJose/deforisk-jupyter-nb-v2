"""Registry mechanics for the shared GEE catalogue."""

import pytest


def test_register_and_get_resolver_roundtrip():
    from spatialrisk.gee import catalogue

    @catalogue.register("dummy_layer")
    def _dummy(aoi_ee, year=None):
        return ("dummy", aoi_ee, year)

    resolver = catalogue.get_resolver("dummy_layer")
    assert resolver is _dummy
    assert resolver("AOI", year=2020) == ("dummy", "AOI", 2020)

    # cleanup so the global registry is not polluted for other tests
    del catalogue.CATALOGUE["dummy_layer"]


def test_get_resolver_unknown_key_raises_keyerror():
    from spatialrisk.gee import catalogue

    with pytest.raises(KeyError, match="no_such_key"):
        catalogue.get_resolver("no_such_key")


def test_register_duplicate_key_raises():
    from spatialrisk.gee import catalogue

    @catalogue.register("dup_key")
    def _first(aoi_ee):
        return 1

    with pytest.raises(ValueError, match="dup_key"):

        @catalogue.register("dup_key")
        def _second(aoi_ee):
            return 2

    del catalogue.CATALOGUE["dup_key"]
