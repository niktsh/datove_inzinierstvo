"""Príjem push správ od iných tímov (webhook). Správy sa ukladajú do data lake tak, ako prišli.

Tím 5 posiela predaj vstupeniek do divadla POST-om na našu adresu
`/webhooks/team_5/<TOKEN>`. Token (`TEAM5_WEBHOOK_TOKEN` v .env) je súčasť adresy: bez neho sa
endpoint tvári, že neexistuje (404), takže sa doň nedá zapisovať náhodne. Veľkosť tela je
obmedzená. Rovnaké telo sa uloží raz (odtlačok obsahu).
"""

import hashlib
import hmac
import logging

from fastapi import APIRouter, HTTPException, Request

from krakow_di.lake.message import LakeMessage
from krakow_di.lake.writer import write_message

log = logging.getLogger(__name__)
router = APIRouter()
MAX_BODY_BYTES = 256 * 1024


async def read_limited(request: Request, limit: int = MAX_BODY_BYTES) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise HTTPException(413, "telo je príliš veľké")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(413, "telo je príliš veľké")
    return bytes(body)


@router.post("/webhooks/team_5/{token}", include_in_schema=False)
async def team5_webhook(token: str, request: Request):
    expected = request.app.state.settings.team5_webhook_token
    if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
        raise HTTPException(404, "Not Found")
    body = await read_limited(request)
    if not body:
        raise HTTPException(400, "prázdne telo")
    msg = LakeMessage(
        team="team_5", channel="webhook", content_type=request.headers.get("content-type"),
        source_ref={"webhook": "team_5", "sha256": hashlib.sha256(body).hexdigest()},
        payload_raw=body,
    )
    async with request.app.state.pool.connection() as conn:
        stored = await write_message(conn, msg)
    log.info("webhook team_5: %d B, %s", len(body), "uložené" if stored else "duplicita")
    return {"ok": True, "stored": stored}
