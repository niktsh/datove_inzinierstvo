"""Adaptér tímu 5: letenky z ich S3 data lake (Cloudflare R2); popis: docs/timy/team_5.md.

Tím 5 ukladá dávky letov ako JSON súbory do bucketu `data-lake` (prefix
`bronze/domain=flights/...`). Adaptér pravidelne vypíše objekty, stiahne nové a každý objekt
uloží ako jednu správu tak, ako je (surové bajty; `payload_json` vyplní writer, keďže ide o JSON).
`source_ref` = `{bucket, key, etag, last_modified}`: ten istý objekt sa uloží raz, zmenený
objekt (iný `etag`) ako nová správa. Divadlo od nich prichádza inak (webhook): pozri
`krakow_di/api/webhooks.py`.
"""

import asyncio
import logging

from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage

log = logging.getLogger(__name__)


def make_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=endpoint_url, aws_access_key_id=access_key,
        aws_secret_access_key=secret_key, region_name="auto",
        config=Config(retries={"max_attempts": 3, "mode": "standard"},
                      connect_timeout=10, read_timeout=60),
    )


class Team5S3Adapter(Adapter):
    channel = "s3"

    def __init__(self, team: str, endpoint_url: str, bucket: str,
                 prefix: str = "bronze/domain=flights/", access_key: str = "",
                 secret_key: str = "", interval_seconds: float = 1800, concurrency: int = 8,
                 client=None, **_ignored):
        self.team, self.endpoint_url, self.bucket, self.prefix = team, endpoint_url, bucket, prefix
        self.access_key, self.secret_key = access_key, secret_key
        self.interval, self.concurrency, self.client = interval_seconds, concurrency, client
        self.seen: set[tuple[str, str]] = set()  # (key, etag) už odovzdané v tomto behu

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        if self.client is None:
            if not self.access_key or not self.secret_key:
                # chýbajúce kľúče v .env: chyba ide cez reštart runnera, ostatné zdroje bežia
                raise RuntimeError(f"{self.name}: chýbajú prístupové kľúče (doplňte ich do .env)")
            self.client = make_s3_client(self.endpoint_url, self.access_key, self.secret_key)
        while not stop.is_set():
            await self._poll(sink, stop)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass

    def _list(self) -> list[dict]:
        objs: list[dict] = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=self.prefix):
            objs.extend(page.get("Contents", []))
        return objs

    def _get(self, key: str) -> tuple[bytes, str | None]:
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read(), resp.get("ContentType")

    async def _poll(self, sink: Sink, stop: asyncio.Event) -> None:
        objs = await asyncio.to_thread(self._list)
        new = sorted((o for o in objs if (o["Key"], o["ETag"]) not in self.seen),
                     key=lambda o: (o["LastModified"], o["Key"]))
        log.info("%s: v bucketi %d objektov, nových %d", self.name, len(objs), len(new))
        for i in range(0, len(new), self.concurrency):
            if stop.is_set():
                return
            batch = new[i:i + self.concurrency]
            bodies = await asyncio.gather(*(asyncio.to_thread(self._get, o["Key"]) for o in batch))
            for o, (body, content_type) in zip(batch, bodies, strict=True):
                await sink(LakeMessage(
                    team=self.team, channel=self.channel, content_type=content_type,
                    source_ref={"bucket": self.bucket, "key": o["Key"], "etag": o["ETag"],
                                "last_modified": o["LastModified"].isoformat()},
                    payload_raw=body,
                ))
                self.seen.add((o["Key"], o["ETag"]))
