# spatialrisk/predictors/jnr.py
"""JNRPredictor — vulnerability map + per-class defrate, single output raster."""

from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Tuple, Union


class JNRPredictor:
    """Bespoke JNR apply collaborator (single output + defrate table).

    rmj functions are injected for testability and default to the real
    ``spatialrisk.rmj`` implementations.
    """

    def __init__(
        self,
        vulnerability_map_fn: Optional[Callable] = None,
        defrate_per_class_fn: Optional[Callable] = None,
    ):
        if vulnerability_map_fn is None or defrate_per_class_fn is None:
            from spatialrisk.rmj import vulnerability_map, deforrate

            vulnerability_map_fn = vulnerability_map_fn or vulnerability_map
            defrate_per_class_fn = defrate_per_class_fn or deforrate.defrate_per_class
        self._vulnerability_map = vulnerability_map_fn
        self._defrate_per_class = defrate_per_class_fn

    def apply(
        self,
        output_file: Union[str, Path],
        defor_file: Union[str, Path],
        forest_file: Union[str, Path],
        forest_edge_file: Union[str, Path],
        subj_file: Union[str, Path],
        dist_bins: Sequence[float],
        time_interval: int,
        period: str,
        blk_rows: int = 128,
        deforate_model: Optional[Union[str, Path]] = None,
        register_prediction: Optional[Callable] = None,
        dataset: Optional[Any] = None,
    ) -> Tuple[Path, Path]:
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        defrate_tab = output_file.parent / f"defrate_cat_bm_{period}.csv"

        self._vulnerability_map(
            forest_file=forest_file,
            forest_edge_file=forest_edge_file,
            dist_bins=dist_bins,
            subj_file=subj_file,
            output_file=output_file,
            blk_rows=blk_rows,
            verbose=False,
        )
        self._defrate_per_class(
            defor_file=defor_file,
            forest_file=forest_file,
            vulnerability_file=output_file,
            time_interval=time_interval,
            tab_file_defrate=defrate_tab,
            deforate_model=(
                Path(deforate_model) if deforate_model is not None else None
            ),
            blk_rows=blk_rows,
        )

        if register_prediction is not None:
            register_prediction(output_file, dataset=dataset, window=None)

        return output_file, defrate_tab
