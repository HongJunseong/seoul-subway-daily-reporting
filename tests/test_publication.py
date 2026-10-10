import json
import pytest
from scripts.verify_report import verify_report
from src.report import generate_daily_report as publication


@pytest.mark.parametrize("suffix", [".md", ".context.json"])
def test_publication_manifest_detects_changed_input_or_report(tmp_path, monkeypatch, suffix):
    context = {
        "target_ymd": "20260201", "usage_ymd": "20260128", "arrival_ymd": "20260131",
        "usage": {"total_count": 680, "top_stations_by_total": []},
        "arrival": {"total_observations": 4, "valid_eta_observations": 4,
                    "max_snapshots_per_station": 2, "line_wait_stats": []},
        "sources": {"usage": {"path": "v2/gold/usage", "delta_version": 2},
                    "arrival": {"path": "v2/gold/arrival", "delta_version": 0}},
    }
    monkeypatch.setattr(publication, "build_report_context", lambda date: context)
    monkeypatch.setenv("SSDR_REPORT_MODE", "template")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    report = publication.generate_daily_report("20260201", save_to_s3=False)
    manifest = verify_report(report)
    assert manifest["report_mode"] == "template"
    assert json.loads(report.with_suffix(".context.json").read_text())["sources"] == context["sources"]
    original = report.read_bytes()
    assert publication.generate_daily_report("20260201", save_to_s3=False).read_bytes() == original
    changed = report.with_suffix(suffix)
    changed.write_bytes(changed.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_report(report)
