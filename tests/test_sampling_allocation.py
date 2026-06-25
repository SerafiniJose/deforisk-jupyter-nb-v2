import pytest

from spatialrisk.sampling import allocation as alloc


def test_equal_splits_total_evenly_capped():
    # 1000 total, 2 classes -> 500 each; class 1 only has 300 -> capped at 300.
    out = alloc.allocate_equal({0: 10_000, 1: 300}, 1000)
    assert out == {0: 500, 1: 300}


def test_proportional_to_class_pixel_counts():
    # class 0 has 90%, class 1 has 10% of pixels; 1000 total.
    out = alloc.allocate_proportional({0: 9000, 1: 1000}, 1000)
    assert out == {0: 900, 1: 100}


def test_proportional_caps_at_availability():
    out = alloc.allocate_proportional({0: 50, 1: 9950}, 1000)
    # class 0 proportional share = 5 (<=50 ok); class 1 = 995 (<=9950 ok)
    assert out == {0: 5, 1: 995}


def test_deforisk_draws_n_per_class_event_classes_present():
    # n interpreted PER class (legacy behaviour): 1000 from each, capped.
    out = alloc.allocate_deforisk({0: 10_000, 1: 700}, 1000)
    assert out == {0: 1000, 1: 700}


def test_adapt_clips_between_10k_and_50k():
    # 1000 samples per 1 Mha. 0.5 Mha -> 500 -> clipped up to 10_000.
    assert alloc.adapt_n_samples(total_pixels=500_000, pixel_area_ha=1.0) == 10_000
    # 100 Mha -> 100_000 -> clipped down to 50_000.
    assert alloc.adapt_n_samples(total_pixels=100_000_000, pixel_area_ha=1.0) == 50_000
    # 30 Mha -> 30_000 (within range).
    assert alloc.adapt_n_samples(total_pixels=30_000_000, pixel_area_ha=1.0) == 30_000


def test_deforisk_adapt_uses_total_forest_area():
    # forestatrisk-faithful: adapt n from TOTAL forest pixels (nfc + ndc, both
    # classes), then draw n from each. total = 60M px * 1 ha = 60 Mha ->
    # 1000 * 60 = 60_000 -> clipped to 50_000 per class.
    out = alloc.allocate_deforisk(
        {0: 30_000_000, 1: 30_000_000}, 1000, adapt=True, pixel_area_ha=1.0
    )
    assert out == {0: 50_000, 1: 50_000}
