# --------------------------------------------------------------
#  dask_ee_vector_export.py
# --------------------------------------------------------------

"""
Utility to export an Earth Engine geometry/FeatureCollection/Feature to a Shapefile using Dask for parallelism.

The helper keeps the following items as explicit function parameters:

* `client` - the :class:`dask.distributed.Client`
* `ee_obj` - an ``ee.Geometry | ee.FeatureCollection | ee.Feature`` instance
* `selectors` - list of property names that should be kept in the output

Everything else (project name, time-outs, etc.) is passed through as keyword
arguments.
"""

# ------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional, Union

import ee  # Earth Engine
from dask.distributed import Client, Future


# ------------------------------------------------------------------
# Public helper
# ------------------------------------------------------------------
def export_vector_with_dask(
    ee_fc: ee.FeatureCollection,
    filename: str,
    selectors: [str] = None,
    overwrite: bool = False,
    client: Client = None,
    project: str = None,
    **kwargs: Any,
) -> Future:
    """
    Export an Earth Engine object to a Shapefile using Dask.

    Parameters
    ----------
    ee_fc : ee.FeatureCollection
        The EE FeatureCollection you want to export.  Must be serialisable via ``serialize()``.
    filename : str
        Path where the exported Shapefile (or ZIP) should be written.
    overwrite : bool, optional
        If False (default), the function will skip export if the file already exists.
        If True, the function will overwrite existing files.
    client : dask.distributed.Client
        The Dask client that will submit the export job.
    project : str | None, optional
        Earth Engine project name.  If omitted the default EE project will be used.
    **kwargs : Any
        Additional keyword arguments forwarded to :func:`geemap.ee_export_vector`.
        Common ones include ``project`` (EE project name), ``timeout``,
        ``keep_zip``, etc.

    Returns:
    -------
    dask.distributed.Future
        Future that resolves to the result of `geemap.ee_export_vector`
        (usually ``None``; you may use it purely for its side effects).
    """
    # ------------------------------------------------------------------
    # 1. Check if file exists and overwrite is False
    # ------------------------------------------------------------------
    if not overwrite and Path(filename).exists():
        logging.warning(
            f"File {filename} already exists and overwrite=False. Skipping export."
        )
        # Return a completed future to continue execution without error
        return client.submit(lambda: None)

    # ------------------------------------------------------------------
    # 2. Ensure Earth Engine is initialised locally
    # ------------------------------------------------------------------
    # if not ee.data._credentials:
    #     ee.Initialize(project=kwargs.get("project"))

    # ------------------------------------------------------------------
    # 3. Serialise the EE object so it can be sent to a worker
    # ------------------------------------------------------------------
    ee_json = ee_fc.serialize()

    # ------------------------------------------------------------------
    # 4. Worker function - runs on a Dask worker
    # ------------------------------------------------------------------
    def _vector_export(
        ee_object_json,
        fn: str,
        sel: Optional[List[str]],
        project: str,
        **kw: Any,
    ) -> Any:
        """
        Minimal wrapper that reinitialises EE and calls `geemap.ee_export_vector`.

        Parameters are intentionally typed to aid static analysis.
        """
        import ee  # Import inside the worker
        import geemap

        ee.Initialize(project=project)

        ee_obj_local = ee.deserializer.fromJSON(ee_object_json)

        return geemap.ee_export_vector(
            ee_object=ee_obj_local,
            filename=fn,
            selectors=sel or [],
            keep_zip=False,  # default used in the example
            timeout=600,
            verbose=False,
            **kw,
        )

    # ------------------------------------------------------------------
    # 4. Submit the task to Dask and return the Future
    # ------------------------------------------------------------------
    return client.submit(
        _vector_export,
        ee_json,
        filename,
        selectors,
        project,
        **kwargs,
    )


# ------------------------------------------------------------------
# Example usage (uncomment for quick test)
# ------------------------------------------------------------------
# if __name__ == "__main__":
#     from dask.distributed import Client
#
#     # Setup Dask client
#     client = Client()
#
#     # Create a simple FeatureCollection as an example
#     fc = ee.FeatureCollection([
#         ee.Feature(ee.Geometry.Point([0, 0]), {"name": "A"}),
#         ee.Feature(ee.Geometry.Point([1, 1]), {"name": "B"})
#     ])
#
#     # Export via Dask
#     future = export_vector_with_dask(
#         client,
#         ee_fc=fc,
#         filename="output.shp",
#         project="my_earthengine_project"   # optional override
#     )
#
#     # Wait for completion (blocks until the job finishes)
#     future.result()
