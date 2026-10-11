"""Bounded GET retries without leaking Seoul keys embedded in URLs."""
from __future__ import annotations
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SourceError(RuntimeError):
    pass


def get_json(url: str, *, session=None, timeout: int = 15) -> dict:
    owned = session is None
    client = session or requests.Session()
    if owned:
        policy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                       allowed_methods=["GET"], respect_retry_after_header=True)
        client.mount("http://", HTTPAdapter(max_retries=policy))
        client.mount("https://", HTTPAdapter(max_retries=policy))
    try:
        response = client.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise SourceError("API returned a non-object JSON payload")
        return payload
    except (requests.RequestException, ValueError):
        raise SourceError("API request failed after bounded retries; check source status and credentials") from None
    finally:
        if owned:
            client.close()
