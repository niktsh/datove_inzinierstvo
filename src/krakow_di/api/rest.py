"""REST rozhranie (`/api/v1/...`): ponuky, história cien, predaje, divadlo, žurnál udalostí."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from krakow_di.api.filters import InvalidFilter, parse_types, to_like

router = APIRouter(prefix="/api/v1")

Limit = Annotated[int, Query(ge=1, le=500, description="počet položiek na stránku")]
Offset = Annotated[int, Query(ge=0, description="posun (stránkovanie)")]


def _where(clauses: list[str], params: list) -> str:
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


async def _page(request: Request, table_sql: str, clauses, params, order, limit, offset) -> dict:
    where = _where(clauses, params)
    async with request.app.state.pool.connection() as conn:
        total = (await (await conn.execute(
            f"SELECT count(*) AS n FROM {table_sql}{where}", params)).fetchone())["n"]
        rows = await (await conn.execute(
            f"SELECT * FROM {table_sql}{where} ORDER BY {order} LIMIT %s OFFSET %s",
            [*params, limit, offset])).fetchall()
    return {"items": rows, "total": total, "limit": limit, "offset": offset}


async def _one(request: Request, sql: str, params: list, what: str) -> dict:
    async with request.app.state.pool.connection() as conn:
        row = await (await conn.execute(sql, params)).fetchone()
    if row is None:
        raise HTTPException(404, f"{what} neexistuje")
    return row


# ---------------------------------------------------------------- letenky
@router.get("/offers", tags=["letenky"], summary="Ponuky leteniek")
async def offers(
    request: Request,
    source: str | None = Query(None, description="travelpayouts | ryanair"),
    origin: str | None = Query(None, min_length=3, max_length=3, description="IATA odletu"),
    status: str | None = Query(None, description="active | sold_out | expired"),
    departure_from: datetime | None = None,
    departure_to: datetime | None = None,
    limit: Limit = 50,
    offset: Offset = 0,
):
    clauses, params = [], []
    for col, val in (("source", source), ("origin_iata", origin and origin.upper()),
                     ("status", status)):
        if val:
            clauses.append(f"{col} = %s")
            params.append(val)
    if departure_from:
        clauses.append("departure_at >= %s")
        params.append(departure_from)
    if departure_to:
        clauses.append("departure_at <= %s")
        params.append(departure_to)
    return await _page(request, "core.flight_offer", clauses, params,
                       "departure_at, offer_id", limit, offset)


@router.get("/offers/{offer_id}", tags=["letenky"], summary="Detail ponuky")
async def offer(request: Request, offer_id: str):
    return await _one(request, "SELECT * FROM core.flight_offer WHERE offer_id = %s",
                      [offer_id], "ponuka")


@router.get("/offers/{offer_id}/history", tags=["letenky"],
            summary="História ceny a miest ponuky (zber aj generátor)")
async def offer_history(request: Request, offer_id: str, limit: Limit = 100, offset: Offset = 0):
    await _one(request, "SELECT 1 FROM core.flight_offer WHERE offer_id = %s", [offer_id],
               "ponuka")
    return await _page(request, "core.flight_offer_history", ["offer_id = %s"], [offer_id],
                       "observed_at, id", limit, offset)


@router.get("/sales", tags=["letenky"], summary="Predané letenky (simulácia generátora)")
async def sales(
    request: Request,
    offer_id: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: Limit = 50,
    offset: Offset = 0,
):
    clauses, params = [], []
    if offer_id:
        clauses.append("offer_id = %s")
        params.append(offer_id)
    if since:
        clauses.append("sold_at >= %s")
        params.append(since)
    if until:
        clauses.append("sold_at <= %s")
        params.append(until)
    return await _page(request, "core.ticket_sale", clauses, params,
                       "sold_at DESC, sale_id", limit, offset)


# ---------------------------------------------------------------- divadlo
@router.get("/theater/performances", tags=["divadlo"], summary="Predstavenia v programe")
async def performances(
    request: Request,
    status: str | None = Query(None, description="on_sale | sold_out | cancelled | past"),
    starts_from: datetime | None = None,
    starts_to: datetime | None = None,
    limit: Limit = 50,
    offset: Offset = 0,
):
    clauses, params = [], []
    if status:
        clauses.append("status = %s")
        params.append(status)
    if starts_from:
        clauses.append("starts_at >= %s")
        params.append(starts_from)
    if starts_to:
        clauses.append("starts_at <= %s")
        params.append(starts_to)
    return await _page(request, "core.theater_performance", clauses, params,
                       "starts_at, performance_id", limit, offset)


@router.get("/theater/performances/{performance_id}", tags=["divadlo"])
async def performance(request: Request, performance_id: str):
    return await _one(request, "SELECT * FROM core.theater_performance WHERE performance_id = %s",
                      [performance_id], "predstavenie")


@router.get("/theater/performances/{performance_id}/snapshots", tags=["divadlo"],
            summary="Snímky dostupnosti miest (ceny v PLN aj EUR)")
async def performance_snapshots(
    request: Request, performance_id: str, limit: Limit = 100, offset: Offset = 0
):
    await _one(request, "SELECT 1 FROM core.theater_performance WHERE performance_id = %s",
               [performance_id], "predstavenie")
    return await _page(request, "core.theater_snapshot", ["performance_id = %s"],
                       [performance_id], "observed_at DESC, id", limit, offset)


@router.get("/theater/sales", tags=["divadlo"],
            summary="Reálne predaje zistené z rozdielu snímok (záporné množstvo = vrátenie)")
async def theater_sales(
    request: Request, performance_id: str | None = None, limit: Limit = 50, offset: Offset = 0
):
    clauses, params = [], []
    if performance_id:
        clauses.append("performance_id = %s")
        params.append(performance_id)
    return await _page(request, "core.theater_sale", clauses, params,
                       "detected_to DESC, sale_id", limit, offset)


# ---------------------------------------------------------------- žurnál udalostí
@router.get("/events", tags=["udalosti"],
            summary="Žurnál všetkých publikovaných udalostí (kurzor podľa seq)")
async def events(
    request: Request,
    types: str | None = Query(None, description="vzory typov, napr. flight.ticket.sold,theater.*"),
    after_seq: Annotated[int, Query(ge=0, description="vrátiť udalosti s vyšším seq")] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
):
    try:
        patterns = parse_types(types)
    except InvalidFilter as e:
        raise HTTPException(422, str(e)) from None
    sql = "SELECT seq, topic, payload AS event FROM core.event_log WHERE seq > %s"
    params: list = [after_seq]
    if patterns:
        sql += " AND event_type LIKE ANY(%s)"
        params.append(to_like(patterns))
    sql += " ORDER BY seq LIMIT %s"
    params.append(limit)
    async with request.app.state.pool.connection() as conn:
        rows = await (await conn.execute(sql, params)).fetchall()
    return {"items": rows, "next_after_seq": rows[-1]["seq"] if len(rows) == limit else None}


@router.get("/stats", tags=["udalosti"], summary="Súhrnné počty dát")
async def stats(request: Request):
    queries = {
        "offers": "SELECT count(*) AS n FROM core.flight_offer",
        "offers_active": "SELECT count(*) AS n FROM core.flight_offer WHERE status = 'active'",
        "ticket_sales": "SELECT count(*) AS n FROM core.ticket_sale",
        "seats_sold": "SELECT coalesce(sum(quantity), 0)::bigint AS n FROM core.ticket_sale",
        "theater_performances": "SELECT count(*) AS n FROM core.theater_performance",
        "theater_snapshots": "SELECT count(*) AS n FROM core.theater_snapshot",
        "theater_sales": "SELECT count(*) AS n FROM core.theater_sale",
        "events": "SELECT count(*) AS n FROM core.event_log",
        "lake_messages": "SELECT count(*) AS n FROM lake.message",
    }
    out = {}
    async with request.app.state.pool.connection() as conn:
        for name, sql in queries.items():
            out[name] = (await (await conn.execute(sql)).fetchone())["n"]
    return out
