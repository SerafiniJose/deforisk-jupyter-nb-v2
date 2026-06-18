# tests/gee/test_ee_confinement.py
"""`import ee` may appear ONLY in the adapter, catalogue, and ee_* helpers."""

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

ALLOWED = {
    "spatialrisk/gee/adapter.py",
    "spatialrisk/gee/catalogue.py",
    "spatialrisk/gee/ee_raster_export.py",
    "spatialrisk/gee/ee_fao_gaul.py",
    "spatialrisk/gee/ee_rasterize_unique_values.py",
    "spatialrisk/gee/dask_ee_raster_export.py",
    "spatialrisk/gee/dask_ee_vector_export.py",
}


def _imports_ee(path: Path) -> bool:
    for line in path.read_text().splitlines():
        s = line.strip()
        if s == "import ee" or s.startswith("import ee ") or s.startswith("import ee,"):
            return True
        if s.startswith("from ee ") or s.startswith("from ee."):
            return True
    return False


def test_ee_not_imported_in_variable_modules():
    offenders = []
    for py in (REPO / "spatialrisk").rglob("*.py"):
        rel = py.relative_to(REPO).as_posix()
        if rel in ALLOWED:
            continue
        if _imports_ee(py):
            offenders.append(rel)
    assert offenders == [], f"unexpected `import ee` in: {offenders}"
