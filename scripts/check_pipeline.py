"""Read-only local data, task-log and optional campaign check with recovery guidance."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.audit_lake import save_audit
from scripts.capture_sources import new_output, write_json
from scripts.summarize_source_observations import read_campaign
from src.common.dates import parse_as_of, validate_ymd, KST
from src.observability.audit import DeltaAuditReader, audit_lake
from src.observability.health import pipeline_health, render_health
from src.observability.task_events import summarize_events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local-root', required=True, type=Path)
    parser.add_argument('--usage-day', required=True, type=validate_ymd)
    for name in ('start', 'end', 'as-of'):
        parser.add_argument('--' + name, required=True, type=parse_as_of)
    parser.add_argument('--data-kind', required=True, choices=['synthetic', 'observed'])
    parser.add_argument('--task-log', action='append', type=Path)
    parser.add_argument('--campaign-dir', type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    quality = audit_lake(DeltaAuditReader(args.local_root), args.usage_day, args.start, args.end, args.as_of)
    quality['data_kind'] = args.data_kind
    tasks = None
    if args.task_log:
        def lines():
            for path in args.task_log:
                with path.open(encoding='utf-8') as stream:
                    yield from stream
        tasks = summarize_events(lines())
    campaign = None
    if args.campaign_dir:
        if args.data_kind != 'observed':
            parser.error('campaign records cannot be attached to synthetic data')
        campaign = read_campaign(args.campaign_dir, args.as_of.replace(tzinfo=KST).isoformat())
    health = pipeline_health(quality, tasks, campaign)
    output = new_output(args.output_dir)
    save_audit(quality, output / 'audit')
    write_json(output / 'health.json', health)
    (output / 'health.md').write_text(render_health(health), encoding='utf-8')
    print(json.dumps({'status': health['status'], 'diagnostic_codes': [d['code'] for d in health['diagnostics']]}, ensure_ascii=False))
    return {'healthy': 0, 'degraded': 1, 'failed': 2}[health['status']]


if __name__ == '__main__':
    raise SystemExit(main())
