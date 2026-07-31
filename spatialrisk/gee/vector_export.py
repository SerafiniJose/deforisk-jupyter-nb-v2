"""Export an Earth Engine FeatureCollection to a local vector file.

Drop-in replacement for ``geemap.ee_export_vector`` (the only geemap function
this project used), so geemap and its dependency tree can stay out of the
deployment environment. Same signature and semantics, minus geemap's
selector-validation ``propertyNames().getInfo()`` round-trip.
"""

import logging
import zipfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

#: Formats ee.FeatureCollection.getDownloadURL accepts.
_ALLOWED_FORMATS = {"csv", "geojson", "json", "kml", "kmz", "shp"}


def ee_export_vector(
    ee_object,
    filename,
    selectors: Optional[list] = None,
    verbose: bool = True,
    keep_zip: bool = False,
    timeout: int = 300,
    proxies: Optional[dict] = None,
):
    """Download ``ee_object`` to ``filename``, format inferred from the extension.

    Args:
        ee_object: ee.FeatureCollection to export.
        filename: Output path; extension must be one of csv, geojson, json,
            kml, kmz or shp (shp downloads a zip and extracts it alongside).
        selectors: Attributes to export. None exports every property of the
            first feature. For geojson the geometry column is always included.
        verbose: Log progress messages.
        keep_zip: For shp, keep the downloaded zip next to the extracted files.
        timeout: Download timeout in seconds.
        proxies: Optional requests proxy mapping.

    Returns:
        Path to the written file (the .shp for shapefile exports).
    """
    import requests

    path = Path(filename).absolute()
    filetype = path.suffix[1:].lower()
    if filetype not in _ALLOWED_FORMATS:
        raise ValueError(
            f"Unsupported extension '{filetype}': "
            f"must be one of {', '.join(sorted(_ALLOWED_FORMATS))}"
        )

    if selectors is None:
        selectors = ee_object.first().propertyNames().getInfo()
    elif not isinstance(selectors, list):
        raise ValueError("selectors must be a list of attribute names")
    if filetype == "geojson":
        selectors = [".geo"] + selectors

    if verbose:
        logger.info("Generating download URL for %s", path.name)
    url = ee_object.getDownloadURL(
        filetype=filetype, selectors=selectors, filename=path.stem
    )

    target = path.with_suffix(".zip") if filetype == "shp" else path
    if verbose:
        logger.info("Downloading %s", url)
    response = requests.get(url, stream=True, timeout=timeout, proxies=proxies)
    if response.status_code != 200:
        raise RuntimeError(
            f"Export of {path.name} failed with HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )
    with open(target, "wb") as fd:
        for chunk in response.iter_content(chunk_size=8192):
            fd.write(chunk)

    if filetype == "shp":
        with zipfile.ZipFile(target) as z:
            z.extractall(target.parent)
        if not keep_zip:
            target.unlink()

    if verbose:
        logger.info("Data downloaded to %s", path)
    return path
