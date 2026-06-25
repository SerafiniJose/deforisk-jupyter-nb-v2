"""Per-class sample-count allocation for stratified sampling.

Each function returns {class_value: n_to_draw}, capped at the available pixel
count for that class.
"""
from typing import Dict, Optional


def _cap(counts: Dict[int, int], requested: Dict[int, float]) -> Dict[int, int]:
    return {c: min(int(round(requested.get(c, 0))), counts[c]) for c in counts}


def allocate_equal(class_counts: Dict[int, int], n_total: int) -> Dict[int, int]:
    """Split n_total evenly across classes (floor), capped at availability."""
    k = len(class_counts)
    if k == 0:
        return {}
    per = n_total // k
    return _cap(class_counts, {c: per for c in class_counts})


def allocate_proportional(class_counts: Dict[int, int], n_total: int) -> Dict[int, int]:
    """Split n_total proportionally to class pixel counts, capped at availability."""
    total = sum(class_counts.values())
    if total == 0:
        return {c: 0 for c in class_counts}
    return _cap(class_counts, {c: n_total * a / total for c, a in class_counts.items()})


def adapt_n_samples(total_pixels: int, pixel_area_ha: float) -> int:
    """forestatrisk adaptive rule: 1000 samples per 1 Mha, clipped to [10k, 50k]."""
    total_area_ha = pixel_area_ha * total_pixels
    nsamp_prop = 1000 * total_area_ha / 1e6
    if nsamp_prop >= 50000:
        return 50000
    if nsamp_prop <= 10000:
        return 10000
    return int(round(nsamp_prop))


def allocate_deforisk(
    class_counts: Dict[int, int],
    n_per_class: int,
    *,
    adapt: bool = False,
    pixel_area_ha: Optional[float] = None,
) -> Dict[int, int]:
    """Legacy-equivalent: draw n_per_class from EACH class (not a split total).

    With adapt=True and a pixel area, n_per_class is replaced by the
    area-adaptive count (see adapt_n_samples). Caps at availability per class.
    """
    n = n_per_class
    if adapt and pixel_area_ha is not None:
        # forestatrisk-faithful (data/sample.py): adapt n from the TOTAL forest
        # area across BOTH classes (farea = (nfc + ndc) * pix_area), then draw n
        # from each class. NOT per-class — that would diverge from legacy.
        n = adapt_n_samples(sum(class_counts.values()), pixel_area_ha)
    return _cap(class_counts, {c: n for c in class_counts})
