"""Read-only metrics HTTP endpoint; never reads credentials, Delta rows or Airflow DB."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.observability.metrics import KINDS, render_metrics


def handler(directory, kind, expected):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == '/healthz':
                content, status = b'ok\n', 200
            elif self.path == '/metrics':
                try:
                    content, status = render_metrics(directory, kind, expected).encode(), 200
                except (OSError, ValueError, TypeError):
                    content, status = b'Metric state unavailable\n', 503
            else:
                content, status = b'Not found\n', 404
            self.send_response(status)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--data-kind', required=True, choices=sorted(KINDS))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9109)
    parser.add_argument('--expected-task', action='append', default=[], help='DAG_ID/TASK_ID')
    args = parser.parse_args()
    expected = [tuple(value.split('/')) for value in args.expected_task]
    if any(len(value) != 2 for value in expected):
        parser.error('expected-task must be DAG_ID/TASK_ID')
    server = ThreadingHTTPServer((args.host, args.port), handler(args.directory, args.data_kind, expected))
    server.serve_forever()


if __name__ == '__main__':
    main()
