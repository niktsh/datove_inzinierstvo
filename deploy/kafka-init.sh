#!/bin/sh
# Jednorazová inicializácia zabezpečenia Kafky: používateľ `teams` (SCRAM-SHA-512) a ACL iba na čítanie.
# Beží v kontajneri apache/kafka cez INTERNAL listener. Idempotentné (opakované spustenie nič nepokazí).
set -eu
BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
USER_NAME="${KAFKA_TEAMS_USER:-teams}"
PASSWORD="${KAFKA_TEAMS_PASSWORD:?nastavte KAFKA_TEAMS_PASSWORD}"
PREFIX="${KAFKA_TOPIC_PREFIX:-krakow}"
BIN=/opt/kafka/bin

echo "Čakám na broker $BOOTSTRAP ..."
i=0
until $BIN/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 60 ] && { echo "broker sa nespustil"; exit 1; }
  sleep 2
done

echo "Vytváram/obnovujem používateľa $USER_NAME (SCRAM-SHA-512)"
$BIN/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=${PASSWORD}]" \
  --entity-type users --entity-name "$USER_NAME"

echo "ACL: ${USER_NAME} smie iba čítať topiky ${PREFIX}.* a používať skupiny team-*"
$BIN/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add \
  --allow-principal "User:${USER_NAME}" --operation Read --operation Describe \
  --topic "${PREFIX}." --resource-pattern-type prefixed
$BIN/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add \
  --allow-principal "User:${USER_NAME}" --operation Read \
  --group "team-" --resource-pattern-type prefixed

$BIN/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --list --principal "User:${USER_NAME}"
echo "Hotovo."
