#!/bin/bash
# sources the .env file and runs the Solara app
# Usage: ./run_solara.sh [filename] [port]
# If no filename is provided, defaults to gui/solara_app.py
# If no port is provided, defaults to 8910

SOLARA_FILE="gui/solara_app.py"
PORT="8910"

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
      echo "Usage: $0 [filename] [--port PORT]"
      exit 0
      ;;
    *)
      SOLARA_FILE="$1"
      shift
      ;;
  esac
done

while IFS= read -r line; do
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

# solara run "$SOLARA_FILE" --port $PORT --no-open
# solara run "$SOLARA_FILE" --port $PORT --no-open --log-level debug
solara run "$SOLARA_FILE" --port $PORT --no-open
