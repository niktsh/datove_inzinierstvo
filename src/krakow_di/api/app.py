"""FastAPI aplikácia: REST + SSE + WebSocket (spustenie: `python -m krakow_di.api`)."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from krakow_di.api import rest, stream
from krakow_di.api.gateway import KafkaGateway
from krakow_di.api.hub import EventHub
from krakow_di.config import Settings, get_settings

log = logging.getLogger(__name__)

DESCRIPTION = """
Dáta Kraków DI: letenky (Travelpayouts, Ryanair; predaje simuluje generátor) a divadlo
(Teatr im. J. Słowackiego; reálne ceny, miesta a predaje).

Hlavný kanál je **Apache Kafka** (pozri `docs/PRE_TIMY.md`); toto API je doplnkový prístup:
- `GET /stream`: Server-Sent Events s filtrom typov a `Last-Event-ID`,
- `WS /ws`: to isté cez WebSocket,
- `/api/v1/...`: REST (ponuky, história, predaje, divadlo, žurnál udalostí).
"""


def create_app(settings: Settings | None = None, start_gateway: bool = True) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = AsyncConnectionPool(
            settings.database_url, min_size=1, max_size=10, open=False,
            kwargs={"row_factory": dict_row},
        )
        await pool.open()
        app.state.pool = pool
        app.state.hub = EventHub()
        app.state.gateway = None
        if start_gateway:
            app.state.gateway = KafkaGateway(settings, pool, app.state.hub)
            await app.state.gateway.start()
        try:
            yield
        finally:
            if app.state.gateway:
                await app.state.gateway.stop()
            await pool.close()

    app = FastAPI(title="Kraków DI", version="1.0.0", description=DESCRIPTION, lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])
    app.include_router(rest.router)
    app.include_router(stream.router)

    @app.get("/health", tags=["systém"], summary="Stav služby")
    async def health(request: Request):
        db = False
        last_seq = None
        try:
            async with request.app.state.pool.connection() as conn:
                row = await (await conn.execute("SELECT max(seq) AS s FROM core.event_log")
                             ).fetchone()
                db, last_seq = True, row["s"]
        except Exception:  # noqa: BLE001 (health musí vždy odpovedať)
            log.exception("health: DB nedostupná")
        gw = request.app.state.gateway
        return {
            "status": "ok" if db else "degraded",
            "db": db,
            "kafka_gateway": bool(gw and gw.connected),
            "subscribers": request.app.state.hub.subscribers,
            "last_event_seq": last_seq,
        }

    @app.get("/", tags=["systém"], include_in_schema=False)
    async def index():
        return {
            "name": "Kraków DI", "docs": "/docs", "openapi": "/openapi.json",
            "stream": "/stream?types=flight.ticket.sold,theater.*", "ws": "/ws",
            "rest": "/api/v1/offers", "health": "/health",
        }

    return app
