"""Vytvorenie topikov kódom (broker má auto.create.topics.enable=false). Idempotentné."""

import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from krakow_di.events.routing import topic_names

RETENTION = {"retention.ms": "-1"}  # uchovať všetko: ktorýkoľvek tím si môže prečítať históriu


async def _describe_with_retry(
    admin: AIOKafkaAdminClient, names: list[str], attempts: int = 10
) -> dict[str, dict[str, str] | None]:
    """Konfigurácie topikov {názov: {kľúč: hodnota}}; None, ak ich broker nevrátil.

    Hneď po vytvorení broker na chvíľu vracia UNKNOWN_TOPIC_OR_PARTITION (kód 3) a prázdnu
    konfiguráciu: v takom prípade sa dopyt zopakuje.
    """
    out: dict[str, dict[str, str] | None] = {n: None for n in names}
    pending = list(names)
    for attempt in range(attempts):
        if not pending:
            break
        if attempt:
            await asyncio.sleep(0.5)
        described = await admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, n) for n in pending]
        )
        pending = []
        for resp in described:
            for res in resp.resources:
                error_code, name, entries = res[0], res[3], res[4]
                if error_code == 0:
                    out[name] = {e[0]: e[1] for e in entries}
                else:
                    pending.append(name)
    return out


async def ensure_topics(bootstrap_servers: str, prefix: str) -> dict[str, str]:
    """Vytvorí chýbajúce topiky a pri existujúcich overí/opraví retention. Vráti {topik: stav}."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    result: dict[str, str] = {}
    try:
        for name, spec in topic_names(prefix).items():
            try:
                response = await admin.create_topics(
                    [NewTopic(name, spec.partitions, 1, topic_configs=dict(RETENTION))]
                )
            except TopicAlreadyExistsError:
                result[name] = "existuje"
                continue
            # broker môže chybu vrátiť aj v odpovedi namiesto výnimky
            code = response.topic_errors[0][1]
            if code == TopicAlreadyExistsError.errno:
                result[name] = "existuje"
            elif code != 0:
                raise KafkaError(f"vytvorenie topiku {name} zlyhalo, kód {code}")
            else:
                result[name] = "vytvorený"
        # Čerstvo vytvorené topiky majú retention už z vytvorenia (a ich konfigurácia sa hneď po
        # vytvorení ešte nemusí vrátiť správne), preto kontrolujeme iba tie, čo už existovali.
        existing = [n for n, state in result.items() if state == "existuje"]
        for name, entries in (await _describe_with_retry(admin, existing)).items():
            if entries is None:
                result[name] += ", konfiguráciu sa nepodarilo overiť"
            elif entries.get("retention.ms") != RETENTION["retention.ms"]:
                await admin.alter_configs(
                    [ConfigResource(ConfigResourceType.TOPIC, name, dict(RETENTION))]
                )
                result[name] += ", retention opravený"
    finally:
        await admin.close()
    return result


async def delete_topics(bootstrap_servers: str, prefix: str) -> None:
    """Pomocná funkcia pre testy: zmaže topiky s daným prefixom."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.delete_topics(list(topic_names(prefix)))
    finally:
        await admin.close()
