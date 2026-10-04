#!/bin/sh
# Vygeneruje vlastnú CA a serverový certifikát pre externý listener Kafky (SASL_SSL).
#   sh scripts/gen_kafka_tls.sh kafka.example.org [IP]
# Výstup v deploy/certs/: ca.crt (tento súbor dajte ostatným tímom), kafka.keystore.p12 (pre broker).
# Pri verejnom nasadení môžete použiť aj certifikát od bežnej CA; stačí dodať PKCS12 keystore.
set -eu
HOST="${1:?použitie: gen_kafka_tls.sh <hostname> [ip]}"
IP="${2:-}"
PASS="${KAFKA_SSL_PASSWORD:-changeit}"
DIR="$(dirname "$0")/../deploy/certs"
mkdir -p "$DIR"
cd "$DIR"

SAN="DNS:${HOST},DNS:localhost,IP:127.0.0.1"
[ -n "$IP" ] && SAN="${SAN},IP:${IP}"

openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -key ca.key -sha256 -days 825 -subj "/CN=Krakow DI CA" -out ca.crt
openssl genrsa -out kafka.key 4096 2>/dev/null
openssl req -new -key kafka.key -subj "/CN=${HOST}" -out kafka.csr
printf "subjectAltName=%s\nextendedKeyUsage=serverAuth\n" "$SAN" > san.ext
openssl x509 -req -in kafka.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out kafka.crt \
  -days 825 -sha256 -extfile san.ext 2>/dev/null
openssl pkcs12 -export -in kafka.crt -inkey kafka.key -certfile ca.crt -name kafka \
  -out kafka.keystore.p12 -passout "pass:${PASS}"
chmod 644 ca.crt kafka.keystore.p12
rm -f kafka.csr san.ext ca.srl
echo "Hotovo: $(pwd)/ca.crt (pre tímy), $(pwd)/kafka.keystore.p12 (pre broker). SAN: ${SAN}"
