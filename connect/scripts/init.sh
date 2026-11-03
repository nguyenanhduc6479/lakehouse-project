#!/bin/bash
# Register every connector in /connectors (<name>.json = its config) with Kafka Connect, then wait
# until each connector and all its tasks are RUNNING.
# Idempotent: PUT /connectors/<name>/config creates the connector or updates it in place.
# Secrets are not substituted here: the JSON keeps `${env:VAR}` and the Connect worker resolves it
# from its own environment (EnvVarConfigProvider), so no password is stored in the config topic.
# Checks the HTTP code at every step — `curl -s` doesn't fail on HTTP 4xx/5xx.
set -euo pipefail

BASE="http://${CONNECT_HOST}:${CONNECT_PORT}"
WAIT_SECONDS=120

# req METHOD URL [JSON file] -> body in /tmp/resp, HTTP code in $CODE
req() {
  if [ -n "${3:-}" ]; then
    CODE=$(curl -s -o /tmp/resp -w '%{http_code}' -X "$1" "$2" \
      -H "Content-Type: application/json" --data-binary "@$3")
  else
    CODE=$(curl -s -o /tmp/resp -w '%{http_code}' -X "$1" "$2")
  fi
}

die() { echo "ERROR: $1 (HTTP ${CODE}): $(cat /tmp/resp 2>/dev/null)"; exit 1; }

shopt -s nullglob
files=(/connectors/*.json)
[ ${#files[@]} -gt 0 ] || { echo "ERROR: no connector configs in /connectors"; exit 1; }

for file in "${files[@]}"; do
  name=$(basename "$file" .json)

  # 201 = created, 200 = updated
  req PUT "${BASE}/connectors/${name}/config" "$file"
  case "$CODE" in
    201) echo "Connector ${name} created." ;;
    200) echo "Connector ${name} updated." ;;
    *) die "registering connector ${name}" ;;
  esac

  # Wait for RUNNING. Status: {"connector":{"state":...},"tasks":[{"state":...}, ...]}; no jq in
  # the image, so count states with grep. A task appears a few seconds after the connector.
  for _ in $(seq "$WAIT_SECONDS"); do
    req GET "${BASE}/connectors/${name}/status"
    if [ "$CODE" = 200 ]; then
      if grep -q '"state":"FAILED"' /tmp/resp; then
        die "connector ${name} FAILED"
      fi
      # `|| true`: grep exits 1 when nothing matches (e.g. UNASSIGNED, no RUNNING yet), which
      # pipefail + set -e would turn into a silent exit of the whole script
      states=$({ grep -o '"state":"[A-Z]*"' /tmp/resp || true; } | wc -l)
      running=$({ grep -o '"state":"RUNNING"' /tmp/resp || true; } | wc -l)
      if [ "$states" -ge 2 ] && [ "$states" -eq "$running" ]; then
        echo "Connector ${name}: connector + $((running - 1)) task(s) RUNNING."
        continue 2
      fi
    fi
    sleep 1
  done
  die "connector ${name} not RUNNING after ${WAIT_SECONDS}s"
done

echo "Kafka Connect setup done."
