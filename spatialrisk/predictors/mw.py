# spatialrisk/predictors/mw.py
"""MWPredictor — moving-window apply, one probability raster per window size."""

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union


class MWPredictor:
    """Bespoke MW apply collaborator (not a block loop; returns dict[str, Path]).

    The rmj processing functions are injected for testability and default to
    the real ``spatialrisk.rmj`` implementations.
    """

    def __init__(
        self,
        set_defor_cat_zero_fn: Optional[Callable] = None,
        defrate_per_cat_fn: Optional[Callable] = None,
    ):
        if set_defor_cat_zero_fn is None or defrate_per_cat_fn is None:
            from spatialrisk.rmj import set_defor_cat_zero, deforrate

            set_defor_cat_zero_fn = set_defor_cat_zero_fn or set_defor_cat_zero
            defrate_per_cat_fn = defrate_per_cat_fn or deforrate.defrate_per_cat
        self._set_defor_cat_zero = set_defor_cat_zero_fn
        self._defrate_per_cat = defrate_per_cat_fn

    def apply(
        self,
        ldefrate_files: Dict[str, Union[str, Path]],
        defor_file: Union[str, Path],
        forest_file: Union[str, Path],
        forest_edge_file: Union[str, Path],
        dist_thresh: float,
        time_interval: int,
        period: str,
        output_folder: Union[str, Path],
        blk_rows: int = 256,
        register_prediction: Optional[Callable] = None,
        dataset: Optional[Any] = None,
    ) -> Dict[str, Path]:
        out_root = Path(output_folder)
        period_dir = out_root / period
        period_dir.mkdir(parents=True, exist_ok=True)

        output_files: Dict[str, Path] = {}
        for win_size_str, ldefrate_file in ldefrate_files.items():
            ldefrate_file = Path(ldefrate_file)
            if not ldefrate_file.exists():
                raise FileNotFoundError(
                    f"ldefrate raster not found: {ldefrate_file}. "
                    "Re-run fit() or restore the file."
                )

            prob_file = period_dir / f"prob_mw_{win_size_str}_{period}.tif"
            defrate_tab = period_dir / f"defrate_cat_mw_{win_size_str}_{period}.csv"

            self._set_defor_cat_zero(
                ldefrate_file=ldefrate_file,
                forest_edge_file=forest_edge_file,
                dist_thresh=dist_thresh,
                output_file=prob_file,
                blk_rows=blk_rows,
                verbose=False,
            )
            self._defrate_per_cat(
                defor_file=defor_file,
                forest_file=forest_file,
                riskmap_file=prob_file,
                time_interval=time_interval,
                tab_file_defrate=defrate_tab,
                blk_rows=blk_rows,
            )

            output_files[win_size_str] = prob_file
            if register_prediction is not None:
                register_prediction(
                    prob_file,
                    dataset=dataset,
                    window=(
                        int(win_size_str)
                        if str(win_size_str).isdigit()
                        else None
                    ),
                )
        return output_files
