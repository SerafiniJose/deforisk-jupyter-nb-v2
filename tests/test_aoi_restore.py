from gui.scripts.aoi_restore import admin_parent_chain


def test_admin0_returns_single_level():
    # Guyana, GAUL 2024 code 197 (from data/GUY manifest).
    assert admin_parent_chain("ADMIN0", "197") == {0: "197"}


def test_unknown_code_returns_empty():
    assert admin_parent_chain("ADMIN0", "99999999") == {}


def test_falsy_code_returns_empty():
    assert admin_parent_chain("ADMIN1", None) == {}
    assert admin_parent_chain("ADMIN2", "") == {}


def test_admin1_includes_parent_level0():
    # Pick the first real ADMIN1 row from pygaul and assert the chain has both
    # levels with consistent codes (data-driven so it survives GAUL updates).
    import pygaul
    df = pygaul._df()
    row = df[df["gaul1_code"].notna()].iloc[0]
    code1 = str(row["gaul1_code"])
    chain = admin_parent_chain("ADMIN1", code1)
    assert chain[1] == code1
    assert chain[0] == str(row["gaul0_code"])


def test_admin2_includes_full_parent_chain():
    import pygaul
    df = pygaul._df()
    row = df[df["gaul2_code"].notna()].iloc[0]
    code2 = str(row["gaul2_code"])
    chain = admin_parent_chain("ADMIN2", code2)
    assert chain[2] == code2
    assert chain[1] == str(row["gaul1_code"])
    assert chain[0] == str(row["gaul0_code"])
