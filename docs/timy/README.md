# Adaptéry cudzích tímov (data lake)

Pre každý cudzí tím máme záznam v [`config/lake_sources.yaml`](../../config/lake_sources.yaml) a (ak
univerzálny adaptér nestačí) vlastný adaptér `src/krakow_di/lake/adapters/team_XX.py` s poznámkou
`docs/timy/team_XX.md` o tom, **ako tím svoje dáta poskytuje** (vlastnými slovami, bez normalizácie).
Pri spolupráci s Claude Code na to slúži príkaz `/novy-tim team_03`.

## Zásady

- Správy ukladáme **tak, ako prišli** (`lake.message.payload_raw` ako bajty, `payload_json` iba ak je
  správa platný JSON). Žiadna normalizácia: to je úloha data warehouse.
- `source_ref` je presný pôvod správy: Kafka `{topic, partition, offset}`, SSE `{url, id}`, REST
  `{url, fetched_at}`. Pri rovnakom `(team, source_ref)` sa správa uloží iba raz (at-least-once zdroje).
  WebSocket nemá stabilné id, preto duplicity neodstraňuje.
- Prihlasovacie údaje iba cez `.env` (v YAML sa uvádza len názov premennej: `username_env`,
  `password_env`, `token_env`).
- Naše vlastné udalosti ukladáme tiež (tím `tuke-krakow`, consumer group `lake-self`).

## Univerzálne adaptéry (bez kódu, stačí YAML)

| `type` | Pre koho | Poznámky |
|---|---|---|
| `kafka` | tím publikuje cez Kafku | consumer group, SASL_SSL, offset sa potvrdzuje až po zápise do DB |
| `sse` | tím ponúka Server-Sent Events | po výpadku sa pripojí s `Last-Event-ID` |
| `ws` | tím ponúka WebSocket | bez stabilného id |
| `rest` | tím ponúka len REST/DB | pravidelné snímky odpovede (`interval_seconds`) |

Príklady sú v `config/lake_sources.yaml` (zakomentované, `enabled: false`).

## Vlastný adaptér tímu (`type: custom`)

Keď tím používa vlastný protokol (napr. MQTT, gRPC) alebo nezvyčajný formát, napíšte
`src/krakow_di/lake/adapters/team_XX.py`:

```python
import asyncio

from krakow_di.lake.adapters.base import Adapter, Sink
from krakow_di.lake.message import LakeMessage


class Team03Adapter(Adapter):
    channel = "mqtt"

    def __init__(self, team: str, username=None, password=None, headers=None, **config):
        self.team, self.config = team, config   # username/password pochádzajú z .env

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        # pripojiť sa k zdroju ich protokolom; pre každú správu:
        #   await sink(LakeMessage(team=self.team, channel=self.channel,
        #                          source_ref={...}, payload_raw=b"...", content_type="..."))
        # potvrdenie zdroja (offset/ack) až PO návrate zo sinku; pri chybe výnimku vyhodiť:
        # runner adaptér reštartuje s backoffom
        ...
```

a v YAML:

```yaml
  - team: team_03
    type: custom
    class: krakow_di.lake.adapters.team_03:Team03Adapter
    username_env: TEAM03_USER
    password_env: TEAM03_PASSWORD
    host: mqtt.team03.example
```

Pre každý adaptér treba test na zaznamenaných vzorkách ich správ (`tests/`).

## Stav tímov

Zatiaľ máme len adaptér vlastných udalostí. Adaptéry ostatných tímov pribudnú, keď zverejnia svoje
rozhrania (protokol, adresa, formát): každý tím si svoje rozhranie navrhuje sám, bez koordinácie.
