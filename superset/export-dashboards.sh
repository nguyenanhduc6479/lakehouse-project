#!/bin/bash
# Host side: export every Superset dashboard (with its charts, datasets and the database entry,
# password masked) into superset/dashboards/ as plain YAML, replacing what is there — then review
# `git diff superset/dashboards` and commit. Imported back on an empty volume by
# superset/scripts/bootstrap.py. Needs the superset container running.
set -euo pipefail
cd "$(dirname "$0")"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Superset logs a lot on stderr: show it only if the export fails
if ! out=$(docker exec superset superset export-dashboards -f /tmp/dashboards-export.zip 2>&1); then
    echo "$out" >&2
    exit 1
fi
docker cp -q superset:/tmp/dashboards-export.zip "$tmp/export.zip"
docker exec superset rm -f /tmp/dashboards-export.zip

# unzip without the zip's root folder (dashboard_export_<timestamp>/), YAML files only
python3 - "$tmp/export.zip" "$tmp/bundle" <<'EOF'
import sys, zipfile
from pathlib import Path, PurePosixPath

src, dst = sys.argv[1], Path(sys.argv[2])
with zipfile.ZipFile(src) as bundle:
    for name in bundle.namelist():
        rel = PurePosixPath(*PurePosixPath(name).parts[1:])
        if rel.suffix not in {".yaml", ".yml"} or ".." in rel.parts:
            continue
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(bundle.read(name))
EOF

# replace the bundle, keep the README
find dashboards -mindepth 1 -maxdepth 1 ! -name README.md -exec rm -rf {} +
cp -r "$tmp/bundle/." dashboards/
echo "exported to superset/dashboards/:"
find dashboards -name '*.yaml' | sort | sed 's/^/  /'
