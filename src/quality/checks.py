"""Blocking contracts: invalid values must not silently become zero counts."""
from __future__ import annotations
from pyspark.sql import DataFrame, functions as F


class DataQualityError(RuntimeError):
    pass


def require_columns(df: DataFrame, required: list[str]) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise DataQualityError(f"Missing required columns: {missing}")


def require_nonempty(df: DataFrame, name: str) -> None:
    if df.isEmpty():
        raise DataQualityError(f"{name}: empty input; refusing to publish")


def reject_rows(df: DataFrame, predicate, name: str) -> None:
    invalid = df.filter(predicate).count()
    if invalid:
        raise DataQualityError(f"{name}: {invalid} invalid rows")


def require_unique(df: DataFrame, keys: list[str], name: str) -> None:
    if not df.groupBy(*keys).count().where(F.col("count") > 1).isEmpty():
        raise DataQualityError(f"{name}: duplicate natural keys {keys}")
