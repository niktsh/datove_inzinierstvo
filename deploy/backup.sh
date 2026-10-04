#!/bin/sh
# Denná záloha PostgreSQL (pg_dump, formát custom) s rotáciou. Beží v slučke v kontajneri `backup`.
# Zálohuje celú databázu vrátane core.event_log a lake.message, takže pokrýva aj históriu udalostí
# a dáta ostatných tímov (Kafku osobitne zálohovať netreba, pozri ARCHITEKTURA.md).
set -eu
DIR="${BACKUP_DIR:-/backups}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-14}"
INTERVAL="${BACKUP_INTERVAL_SECONDS:-86400}"

while true; do
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  file="$DIR/krakow_di_${stamp}.dump"
  if pg_dump -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -Fc -f "$file.tmp"; then
    mv "$file.tmp" "$file"
    echo "$(date -u +%FT%TZ) záloha OK: $file ($(du -h "$file" | cut -f1))"
  else
    rm -f "$file.tmp"
    echo "$(date -u +%FT%TZ) ZÁLOHA ZLYHALA" >&2
  fi
  find "$DIR" -name 'krakow_di_*.dump' -mtime +"$KEEP_DAYS" -delete
  [ "${BACKUP_ONCE:-0}" = "1" ] && exit 0
  sleep "$INTERVAL"
done
