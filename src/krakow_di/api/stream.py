"""Brána streamu: Server-Sent Events (`GET /stream`) a WebSocket (`/ws`)."""

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

from krakow_di.api.feed import event_feed
from krakow_di.api.filters import InvalidFilter, parse_types

router = APIRouter()
KEEPALIVE_SECONDS = 15


def _patterns(raw: str | None) -> list[str]:
    try:
        return parse_types(raw)
    except InvalidFilter as e:
        raise HTTPException(422, str(e)) from None


def _last_seq(header: str | None, query: int | None) -> int | None:
    if query is not None:
        return query
    if header is not None and header.strip():
        try:
            return int(header)
        except ValueError:
            msg = "Last-Event-ID musí byť číslo (seq poslednej udalosti)"
            raise HTTPException(422, msg) from None
    return None


def sse_frame(seq: int, event: dict) -> str:
    data = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    return f"id: {seq}\nevent: {event['event_type']}\ndata: {data}\n\n"


async def _with_keepalive(agen: AsyncIterator, interval: float) -> AsyncIterator:
    """Prepošle položky a pri tichu každých `interval` s vyšle None (komentár keepalive)."""
    task = asyncio.ensure_future(agen.__anext__())
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=interval)
            if not done:
                yield None
                continue
            try:
                item = task.result()
            except StopAsyncIteration:
                return
            yield item
            task = asyncio.ensure_future(agen.__anext__())
    finally:
        task.cancel()


@router.get("/stream", tags=["stream"], summary="Server-Sent Events: živý prúd udalostí")
async def stream(
    request: Request,
    types: str | None = Query(
        None, description="vzory typov oddelené čiarkou, napr. flight.ticket.sold,theater.*"),
    last_event_id: int | None = Query(
        None, description="to isté ako hlavička Last-Event-ID: pošle udalosti s vyšším seq"),
    max: int | None = Query(None, ge=1, description="skončiť po N udalostiach (na skúšanie)"),
):
    patterns = _patterns(types)
    last = _last_seq(request.headers.get("last-event-id"), last_event_id)
    state = request.app.state

    async def body() -> AsyncIterator[str]:
        sub = state.hub.subscribe()
        sent = 0
        try:
            yield "retry: 3000\n\n"
            feed = _with_keepalive(
                event_feed(state.pool, state.hub, patterns, last, sub), KEEPALIVE_SECONDS)
            async for item in feed:
                if item is None:
                    yield ": keepalive\n\n"
                    continue
                yield sse_frame(*item)
                sent += 1
                if max is not None and sent >= max:
                    return
        finally:
            state.hub.unsubscribe(sub)

    return StreamingResponse(
        body(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.websocket("/ws")
async def websocket_stream(
    ws: WebSocket,
    types: str | None = None,
    last_event_id: int | None = None,
    max: int | None = None,
):
    try:
        patterns = parse_types(types)
    except InvalidFilter as e:
        await ws.close(code=1008, reason=str(e))
        return
    await ws.accept()
    state = ws.app.state
    sub = state.hub.subscribe()
    feed = event_feed(state.pool, state.hub, patterns, last_event_id, sub)
    sent = 0
    next_item = asyncio.ensure_future(feed.__anext__())
    disconnected = asyncio.ensure_future(ws.receive_text())  # klient zavrel spojenie
    try:
        while True:
            done, _ = await asyncio.wait(
                {next_item, disconnected}, return_when=asyncio.FIRST_COMPLETED)
            if disconnected in done:
                break
            try:
                seq, event = next_item.result()
            except StopAsyncIteration:
                break
            await ws.send_json({"seq": seq, "event": event})
            sent += 1
            if max is not None and sent >= max:
                break
            next_item = asyncio.ensure_future(feed.__anext__())
        await ws.close()
    except WebSocketDisconnect:
        pass
    finally:
        next_item.cancel()
        disconnected.cancel()
        state.hub.unsubscribe(sub)
