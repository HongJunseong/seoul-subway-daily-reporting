from copy import deepcopy
import pytest
from scripts.capture_kubernetes_files import compare_snapshots


def test_gold_commit_versions_may_advance_but_business_rows_must_match():
    before = {"gold/example": {"version": 0, "rows": 3, "sha256": "same"}}
    after = deepcopy(before)
    after["gold/example"]["version"] = 1
    compare_snapshots(before, after)
    after["gold/example"]["sha256"] = "changed"
    with pytest.raises(ValueError, match="rows"):
        compare_snapshots(before, after)


def test_unchanged_bronze_values_do_not_hide_an_extra_bronze_commit():
    before = {"bronze/example": {"version": 0, "rows": 3, "sha256": "same"}}
    after = deepcopy(before)
    after["bronze/example"]["version"] = 1
    with pytest.raises(ValueError, match="Bronze"):
        compare_snapshots(before, after)
