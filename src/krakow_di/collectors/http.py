import logging
import time
from collections.abc import Callable

import httpx

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
# Čestný, identifikovateľný klient (bez napodobňovania prehliadača).
USER_AGENT = "tuke-di-krakow/0.1 (student data engineering project)"

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
    """GET s opakovaním pri 429/5xx/sieťových chybách (exponenciálny backoff, Retry-After).

    Vráti poslednú odpoveď alebo None, ak všetky pokusy zlyhali na úrovni siete.
    Iné stavy (403, 409, ...) sa vracajú ihneď: zvyčajne znamenajú blokáciu
    a opakovanie by zdroj iba zbytočne zaťažovalo.
    """
    resp = None
    for attempt in range(max_attempts):
        try:
            resp = client.get(url, params=params, headers=headers)
        except httpx.TransportError as e:
            log.warning("sieťová chyba %s (pokus %d)", type(e).__name__, attempt + 1)
            resp = None
        else:
            if resp.status_code not in RETRY_STATUSES:
                return resp
            log.warning("HTTP %d (pokus %d)", resp.status_code, attempt + 1)
        if attempt + 1 < max_attempts:
            delay = 2.0**attempt
            if resp is not None and resp.headers.get("Retry-After", "").isdigit():
                delay = max(delay, float(resp.headers["Retry-After"]))
            sleep(delay)
    return resp
