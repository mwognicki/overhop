#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <type> <slug> [message]"
  echo "Types: added|changed|fixed|arch|protocol|llm|docs"
  exit 1
fi

type="$1"
slug="$2"
message="${3:-Describe change here}"
ts="$(date +%Y%m%d-%H%M)"

case "$type" in
  added) section="Added" ;;
  changed) section="Changed" ;;
  fixed) section="Fixed" ;;
  arch) section="Architecture" ;;
  protocol) section="Protocol" ;;
  llm) section="LLM Workflow" ;;
  docs) section="Changed" ;;
  *)
    echo "Invalid type: $type"
    exit 1
    ;;
esac

path=".changelog/unreleased/${ts}-${type}-${slug}.md"
cat > "$path" <<FRAG
### ${section}
- ${message}
FRAG

echo "Created ${path}"
