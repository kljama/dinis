#!/bin/sh
# Makes a self-signed certificate for Nginx when the certificate directory has none.
# It never replaces files that exist. To use a signed certificate, put it in the directory
# as tls.crt (server certificate, then the intermediate certificates) and tls.key (private key).
set -eu

DIR=/certs
CRT="$DIR/tls.crt"
KEY="$DIR/tls.key"

if [ -s "$CRT" ] && [ -s "$KEY" ]; then
    echo "nginx-certs: $CRT and $KEY exist, no change"
    exit 0
fi
if [ -e "$CRT" ] || [ -e "$KEY" ]; then
    echo "nginx-certs: only one of tls.crt and tls.key exists (or one is empty) in the certificate directory." >&2
    echo "nginx-certs: add the missing file, or remove both to get a new self-signed certificate." >&2
    exit 1
fi

CN="${TLS_SELF_SIGNED_CN:-localhost}"
SAN="${TLS_SELF_SIGNED_SAN:-DNS:localhost,IP:127.0.0.1}"
DAYS="${TLS_SELF_SIGNED_DAYS:-825}"

umask 077
openssl req -x509 -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -days "$DAYS" -subj "/CN=$CN" -addext "subjectAltName=$SAN" \
    -keyout "$KEY.tmp" -out "$CRT.tmp"
chmod 0644 "$CRT.tmp"
chmod 0600 "$KEY.tmp"
# Give the files to the owner of the directory, so the host user can replace them without root
chown "$(stat -c %u:%g "$DIR")" "$CRT.tmp" "$KEY.tmp"
mv "$KEY.tmp" "$KEY"
mv "$CRT.tmp" "$CRT"
echo "nginx-certs: made a self-signed certificate for CN=$CN, $SAN, valid $DAYS days"
