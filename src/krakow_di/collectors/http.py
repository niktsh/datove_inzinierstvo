import logging
import time
from collections.abc import Callable

import httpx

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

log = logging.getLogger(__name__)


def get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict,
    headers: dict | None = None,
    max_attempts: int = 4,
    sleep: Callable[[float], None] = time.sleep,
) -> httpx.Response | None:
    """GET with retry on 429/5xx/network errors (exponential backoff, honours Retry-After).

    Returns the last response, or None if every attempt failed at the network level.
    Other statuses (403, 409, ...) are returned immediately: they usually mean a block,
    and retrying would only hammer the source.
    """
    resp = None
    for attempt in range(max_attempts):
        try:
            resp = client.get(url, params=params, headers=headers)
        except httpx.TransportError as e:
            log.warning("network error %s (attempt %d)", type(e).__name__, attempt + 1)
            resp = None
        else:
            if resp.status_code not in RETRY_STATUSES:
                return resp
            log.warning("HTTP %d (attempt %d)", resp.status_code, attempt + 1)
        if attempt + 1 < max_attempts:
            delay = 2.0**attempt
            if resp is not None and resp.headers.get("Retry-After", "").isdigit():
                delay = max(delay, float(resp.headers["Retry-After"]))
            sleep(delay)
    return resp
