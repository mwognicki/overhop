#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 [--output <dir>] [--all-features] [--private]

Generates Rust API docs using cargo doc and copies them into docs/.
Defaults:
  output directory: docs/rustdoc
  mode: --no-deps

Examples:
  $0
  $0 --all-features
  $0 --private --output docs/api
USAGE
}

output_dir="docs/rustdoc"
all_features="false"
private_items="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      output_dir="${2:-}"
      shift 2
      ;;
    --all-features)
      all_features="true"
      shift
      ;;
    --private)
      private_items="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

doc_cmd=(cargo doc --no-deps)
if [[ "$all_features" == "true" ]]; then
  doc_cmd+=(--all-features)
fi
if [[ "$private_items" == "true" ]]; then
  doc_cmd+=(--document-private-items)
fi

echo "Generating docs with: ${doc_cmd[*]}"
"${doc_cmd[@]}"

mkdir -p "$output_dir"

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete target/doc/ "$output_dir"/
else
  rm -rf "$output_dir"
  mkdir -p "$output_dir"
  cp -R target/doc/. "$output_dir"/
fi

echo "Docs generated at: $output_dir"
if [[ -f "$output_dir/index.html" ]]; then
  echo "Open: $output_dir/index.html"
fi
