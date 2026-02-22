#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: $0 [--base <sha>] [--head <sha>] [--output <path>]

Generates a PR body from .changelog/unreleased fragments.
- If --base/--head are provided, only fragments changed in that range are used.
- Otherwise, script tries to use current branch diff vs origin/main (or main), with fallback to all fragments.
- Output defaults to stdout.
USAGE
}

base_sha=""
head_sha=""
output_path=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      base_sha="${2:-}"
      shift 2
      ;;
    --head)
      head_sha="${2:-}"
      shift 2
      ;;
    --output)
      output_path="${2:-}"
      shift 2
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

if [[ -n "$base_sha" && -z "$head_sha" ]] || [[ -z "$base_sha" && -n "$head_sha" ]]; then
  echo "Both --base and --head must be provided together" >&2
  exit 1
fi

fragments=()
while IFS= read -r fragment; do
  [[ -n "$fragment" ]] && fragments+=("$fragment")
done < <(
  if [[ -n "$base_sha" ]]; then
    git diff --name-only "$base_sha...$head_sha" -- '.changelog/unreleased/*.md' | sort
  else
    base_ref=""
    if git rev-parse --verify --quiet origin/main >/dev/null; then
      base_ref="origin/main"
    elif git rev-parse --verify --quiet main >/dev/null; then
      base_ref="main"
    fi

    if [[ -n "$base_ref" ]]; then
      merge_base="$(git merge-base "$base_ref" HEAD || true)"
      if [[ -n "$merge_base" ]]; then
        git diff --name-only "$merge_base...HEAD" -- '.changelog/unreleased/*.md' | sort
      else
        find .changelog/unreleased -maxdepth 1 -type f -name '*.md' | sort
      fi
    else
      find .changelog/unreleased -maxdepth 1 -type f -name '*.md' | sort
    fi
  fi
)

if [[ ${#fragments[@]} -eq 0 ]]; then
  echo "No changelog fragments found for PR body generation" >&2
  exit 1
fi

summary_bullets=()
scope_bullets=()
notes_bullets=()
sections_seen=()

contains() {
  local needle="$1"
  shift
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

for fragment in "${fragments[@]}"; do
  if [[ ! -f "$fragment" ]]; then
    continue
  fi

  current_section=""
  while IFS= read -r line; do
    if [[ "$line" =~ ^###[[:space:]]+(.+)$ ]]; then
      current_section="${BASH_REMATCH[1]}"
      if ! contains "$current_section" "${sections_seen[@]-}"; then
        sections_seen+=("$current_section")
      fi
      continue
    fi

    if [[ "$line" =~ ^-[[:space:]]+(.+) ]]; then
      bullet="${BASH_REMATCH[1]}"
      case "$current_section" in
        Added|Changed|Fixed)
          summary_bullets+=("$bullet")
          ;;
        Architecture|Protocol|LLM\ Workflow)
          notes_bullets+=("$bullet")
          ;;
        *)
          summary_bullets+=("$bullet")
          ;;
      esac
    fi
  done < "$fragment"

done

for fragment in "${fragments[@]}"; do
  scope_bullets+=("\`$fragment\`")
done

build_body() {
  echo "## Summary"
  echo
  if [[ ${#summary_bullets[@]} -gt 0 ]]; then
    for bullet in "${summary_bullets[@]}"; do
      echo "- $bullet"
    done
  else
    echo "- Updated changelog fragments (no summary bullets found)."
  fi

  echo
  echo "## Scope"
  echo
  echo "- In scope fragments:"
  for fragment in "${scope_bullets[@]}"; do
    echo "- $fragment"
  done
  if [[ ${#sections_seen[@]} -gt 0 ]]; then
    echo "- Covered sections: ${sections_seen[*]}"
  fi

  echo
  echo "## Changelog Fragment"
  echo
  echo "- [x] I added fragment(s) in \`.changelog/unreleased/*.md\`."
  for fragment in "${scope_bullets[@]}"; do
    echo "- $fragment"
  done

  echo
  echo "## Validation"
  echo
  echo "- [ ] \`cargo test\` (if applicable)"
  echo "- [ ] CI checks passed"
  echo "- [ ] Manual checks completed"

  echo
  echo "## Notes / Tradeoffs"
  echo
  if [[ ${#notes_bullets[@]} -gt 0 ]]; then
    for bullet in "${notes_bullets[@]}"; do
      echo "- $bullet"
    done
  else
    echo "- No explicit tradeoffs captured in changelog fragments."
  fi
}

if [[ -n "$output_path" ]]; then
  build_body > "$output_path"
  echo "Generated PR body at $output_path"
else
  build_body
fi
