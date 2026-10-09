"""Adaptér tímu 4 (letenky aj divadlo cez REST); popis: docs/timy/team_4.md.

Ich rozhranie sú dva REST zdroje: divadlo (Supabase PostgREST, pohľady `tickets_sold` a
`theatre_availability_latest`, kľúč v hlavičke `apikey`) a letenky (FastAPI na Renderi, vždy
iba posledná dávka letov). Odpovede ukladáme tak, ako prišli (surové bajty). Zdroje sú dva
REST kanály jedného tímu, preto sa líšia názvom kanála (`rest_theatre`, `rest_flights`):
runner vyžaduje jedinečnú dvojicu tím/kanál. Duplicity odstraňuje odtlačok obsahu (`sha256`
v `source_ref`): nezmenená odpoveď sa uloží raz, zmena ako nová správa. Žiadna normalizácia.
"""

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime

import httpx

from krakow_di.collectors.http import USER_AGENT
from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage

log = logging.getLogger(__name__)


class Team4RestAdapter(Adapter):
    """Pravidelne číta zoznam endpointov. Endpoint s `paginate: true` stránkuje `limit`/`offset`."""

    def __init__(self, team: str, channel: str, endpoints: list[dict],
                 interval_seconds: float = 21600, page_size: int = 1000, max_pages: int = 20,
                 timeout_seconds: float = 90, headers: dict | None = None,
                 client: httpx.AsyncClient | None = None, **_ignored):
        self.team, self.channel, self.endpoints = team, channel, endpoints
        self.interval, self.page_size, self.max_pages = interval_seconds, page_size, max_pages
        self.timeout, self.headers, self.client = timeout_seconds, headers or {}, client

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        empty = [k for k, v in self.headers.items() if not v]
        if empty:  # napr. chýba kľúč v .env: chyba ide cez reštart runnera, ostatné zdroje bežia
            raise RuntimeError(f"{self.name}: prázdna hlavička {empty} (doplňte kľúč do .env)")
        client = self.client or httpx.AsyncClient(timeout=self.timeout)
        headers = {"User-Agent": USER_AGENT, **self.headers}
        while not stop.is_set():
            for ep in self.endpoints:
                await self._poll(client, headers, ep, sink)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass

    async def _poll(self, client, headers, ep: dict, sink: Sink) -> None:
        pages = self.max_pages if ep.get("paginate") else 1
        for page in range(pages):
            params = dict(ep.get("params") or {})
            offset = page * self.page_size
            if ep.get("paginate"):
                params.update(limit=self.page_size, offset=offset)
            resp = await client.get(ep["url"], params=params, headers=headers)
            if resp.status_code // 100 != 2:
                log.warning("%s: %s vrátil HTTP %s, odpoveď sa neukladá",
                            self.name, ep["url"], resp.status_code)
                return
            rows = _row_count(resp)
            if page > 0 and rows == 0:
                return  # prázdna stránka za koncom nie je dátum
            await sink(LakeMessage(
                team=self.team, channel=self.channel,
                content_type=resp.headers.get("content-type"),
                source_ref={"url": ep["url"], "offset": offset if ep.get("paginate") else None,
                            "sha256": hashlib.sha256(resp.content).hexdigest()},
                payload_raw=resp.content, received_at=datetime.now(UTC),
            ))
            if rows < self.page_size:
                return


def _row_count(resp: httpx.Response) -> int:
    """Počet riadkov v odpovedi: pole (Supabase) alebo objekt s poľom `flights` (Render)."""
    try:
        data = json.loads(resp.content)
    except ValueError:
        return 0
    if isinstance(data, list):
        return len(data)
    return len(data.get("flights", [])) if isinstance(data, dict) else 0
