#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-docs/rustdoc}"

if [[ -d "$output_dir" ]]; then
  rm -rf "$output_dir"
  echo "Removed $output_dir"
else
  echo "Nothing to clean: $output_dir"
fi
