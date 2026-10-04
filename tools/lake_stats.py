#!/usr/bin/env python3
"""Report data lake: koľko správ od ktorého tímu a kanála (za posledných N hodín a spolu).

    uv run python tools/lake_stats.py            # posledných 24 hodín
    uv run python tools/lake_stats.py --hours 6
"""

import argparse

from krakow_di.db import connect

QUERY = """
SELECT team, channel,
       count(*) AS total,
       count(*) FILTER (WHERE received_at > now() - make_interval(hours => %s)) AS recent,
       count(*) FILTER (WHERE payload_json IS NULL) AS not_json,
       pg_size_pretty(sum(octet_length(payload_raw))::bigint) AS raw_size,
       min(received_at) AS first_at,
       max(received_at) AS last_at
FROM lake.message
GROUP BY team, channel
ORDER BY team, channel
"""


def format_report(rows: list[dict], hours: int) -> str:
    if not rows:
        return "data lake je zatiaľ prázdny"
    head = f"{'tím':<18}{'kanál':<7}{f'za {hours} h':>10}{'spolu':>10}{'nie JSON':>10}" \
           f"{'veľkosť':>11}  posledná správa"
    lines = [head, "-" * len(head)]
    for r in rows:
        last = r["last_at"].strftime("%Y-%m-%d %H:%M:%S") if r["last_at"] else "-"
        lines.append(f"{r['team']:<18}{r['channel']:<7}{r['recent']:>10}{r['total']:>10}"
                     f"{r['not_json']:>10}{r['raw_size'] or '0 bytes':>11}  {last}")
    lines.append(f"\nspolu: {sum(r['total'] for r in rows)} správ od "
                 f"{len({r['team'] for r in rows})} tímov")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Štatistika data lake")
    p.add_argument("--hours", type=int, default=24)
    args = p.parse_args()
    with connect() as conn:
        rows = conn.execute(QUERY, (args.hours,)).fetchall()
    print(format_report(rows, args.hours))


if __name__ == "__main__":
    main()
