from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import signal
import threading
import time
from types import SimpleNamespace

import pytest

from src.common.deadline import collection_deadline
from src.common.http import SourceError, SourceRetry, get_json


@contextmanager
def source_server():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.server.calls += 1
            if self.path.startswith('/redirect'):
                self.send_response(302)
                self.send_header('Location', '/ok')
                self.end_headers()
                return
            if self.path.startswith('/retry'):
                self.send_response(503)
                self.send_header('Retry-After', '3600')
                self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            try:
                if self.path.startswith('/slow'):
                    for _ in range(100):
                        self.wfile.write(b' ')
                        self.wfile.flush()
                        time.sleep(0.02)
                self.wfile.write(b'{"ok": true}')
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.calls = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_slow_drip_exceeds_wall_clock_despite_regular_socket_activity():
    previous = signal.getsignal(signal.SIGALRM)
    with source_server() as (server, url):
        started = time.monotonic()
        with pytest.raises(SourceError, match='wall-clock deadline') as caught:
            with collection_deadline(0.15):
                get_json(url + '/slow/private-key', timeout=1)
        assert time.monotonic() - started < 1.5
        assert 'private-key' not in str(caught.value)
        assert signal.getitimer(signal.ITIMER_REAL) == (0, 0)
        assert signal.getsignal(signal.SIGALRM) is previous
        with collection_deadline(1):
            assert get_json(url + '/ok') == {'ok': True}
        assert server.calls == 2


def test_excessive_retry_after_stops_without_sleep_or_early_retry():
    with source_server() as (server, url):
        started = time.monotonic()
        with pytest.raises(SourceError, match='Retry-After'):
            get_json(url + '/retry/private-key')
        assert server.calls == 1
        assert time.monotonic() - started < 1


def test_redirect_does_not_expand_get_budget_or_forward_credential_url():
    with source_server() as (server, url):
        with pytest.raises(SourceError, match='redirect rejected'):
            get_json(url + '/redirect/private-key')
        assert server.calls == 1


def test_short_retry_after_and_exponential_wait_remain_bounded(monkeypatch):
    sleeps = []
    monkeypatch.setattr('urllib3.util.retry.time.sleep', sleeps.append)
    retry = SourceRetry(total=3, backoff_factor=1, backoff_max=5)
    assert retry.sleep_for_retry(SimpleNamespace(headers={'Retry-After': '2'}))
    assert sleeps == [2]
    assert not retry.sleep_for_retry(SimpleNamespace(headers={}))
    for _ in range(3):
        retry = retry.increment(method='GET', error=OSError('private-key'))
        assert retry.get_backoff_time() <= 5


def test_pagination_uses_one_collection_budget_and_discards_partial_result(monkeypatch):
    from src.ingestion import ingest_subway_usage_daily as usage
    monkeypatch.setenv('SEOUL_PASSENGER_API_KEY', 'private-key')
    calls = []

    def response(url, **kwargs):
        calls.append(1)
        time.sleep(0.08)
        row = {'USE_YMD': '20260128', 'SBWY_ROUT_LN_NM': '2', 'SBWY_STNS_NM': str(len(calls))}
        return {'CardSubwayStatsNew': {'RESULT': {'CODE': 'INFO-000'}, 'list_total_count': 3, 'row': [row]}}

    monkeypatch.setattr(usage, 'get_json', response)
    with pytest.raises(SourceError, match='wall-clock deadline'):
        with collection_deadline(0.12):
            usage.fetch_subway_usage_daily.__wrapped__('20260128', page_size=1)
    assert len(calls) == 2


def test_normal_exception_restores_timer_and_handler():
    previous = signal.getsignal(signal.SIGALRM)
    with pytest.raises(ValueError):
        with collection_deadline(1):
            raise ValueError('invalid input')
    assert signal.getsignal(signal.SIGALRM) is previous
    assert signal.getitimer(signal.ITIMER_REAL) == (0, 0)


def test_existing_process_timer_is_not_overwritten():
    signal.setitimer(signal.ITIMER_REAL, 10)
    try:
        with pytest.raises(RuntimeError, match='active process timer'):
            with collection_deadline(1):
                pytest.fail('must not enter')
        assert 5 < signal.getitimer(signal.ITIMER_REAL)[0] <= 10
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


def test_thread_call_fails_before_starting_collection():
    with ThreadPoolExecutor(1) as executor:
        with pytest.raises(RuntimeError, match='main-thread'):
            executor.submit(lambda: collection_deadline(1).__enter__()).result()


@pytest.mark.parametrize('seconds', [0, -1, True, float('nan'), float('inf')])
def test_bad_deadline_is_rejected(seconds):
    with pytest.raises(ValueError):
        with collection_deadline(seconds):
            pytest.fail('must not enter')
