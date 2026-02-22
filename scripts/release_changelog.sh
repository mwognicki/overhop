#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <version> <YYYY-MM-DD>"
  exit 1
fi

version="$1"
release_date="$2"
frag_dir=".changelog/unreleased"

if [[ ! -d "$frag_dir" ]]; then
  echo "Missing $frag_dir"
  exit 1
fi

mapfile -t fragments < <(find "$frag_dir" -maxdepth 1 -type f -name '*.md' | sort)

if [[ ${#fragments[@]} -eq 0 ]]; then
  echo "No changelog fragments found in $frag_dir"
  exit 1
fi

tmp_file="$(mktemp)"
{
  echo "## [$version] - $release_date"
  echo
  for f in "${fragments[@]}"; do
    cat "$f"
    echo
  done
} > "$tmp_file"

awk -v entry_file="$tmp_file" '
BEGIN {
  inserted = 0
}
{
  print $0
  if (!inserted && $0 ~ /^## \[Unreleased\]/) {
    print ""
    while ((getline line < entry_file) > 0) {
      print line
    }
    close(entry_file)
    inserted = 1
  }
}
' CHANGELOG.md > CHANGELOG.md.new

mv CHANGELOG.md.new CHANGELOG.md
rm -f "$tmp_file"

for f in "${fragments[@]}"; do
  rm -f "$f"
done

touch "$frag_dir/.gitkeep"

echo "Released changelog fragments into CHANGELOG.md as [$version] - $release_date"
