#!/bin/bash
# sources the .env file and runs ui.ipynb via voila — the SEPAL app entry path.
# Usage: ./run_ui.sh [--port PORT]
# If no port is provided, defaults to 8911 (run_solara.sh uses 8910)

PORT="8911"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="$2"
      shift 2
      ;;
    --port=*)
      PORT="${1#--port=}"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--port PORT]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Run from the script's own directory so relative paths resolve
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Make the module root importable (so `import gui...` works)
export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"

if [[ -f .env ]]; then
  while IFS= read -r line || [[ -n $line ]]; do
    [[ $line =~ ^#.*$ || -z $line ]] && continue

    if [[ $line =~ ^([^=]+)=(.*)$ ]]; then
      name="${BASH_REMATCH[1]}"
      value="${BASH_REMATCH[2]}"

      # Remove quotes if present
      value="${value#\'}"
      value="${value%\'}"
      value="${value#\"}"
      value="${value%\"}"

      export "$name=$value"
    fi
  done < .env
else
  echo "Note: no .env file found in $SCRIPT_DIR, skipping env load."
fi

# GDAL writes its temp files to the CWD when CPL_TMPDIR/TMPDIR/TEMP are unset,
# and on SEPAL the CWD is the read-only shared module mount -- so point it at a
# writable per-user scratch dir. Set after .env so an operator value wins; the
# uid suffix keeps us off another user's directory on a shared /tmp.
export CPL_TMPDIR="${CPL_TMPDIR:-${TMPDIR:-/tmp}/spatial_risk_gdal_$(id -u)}"
mkdir -p "$CPL_TMPDIR"

# voila ui.ipynb --port=$PORT --no-browser --show_tracebacks=True
voila ui.ipynb --port=$PORT --no-browser
