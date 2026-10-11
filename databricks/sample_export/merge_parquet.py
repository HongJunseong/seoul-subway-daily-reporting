"""Retired unsafe export entry point: use export_from_s3.py instead."""
raise SystemExit(
    "Direct reads of Delta internal Parquet are unsupported. "
    "Use export_from_s3.py with the Delta table ROOT and an explicit date/version."
)
