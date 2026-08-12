#!/usr/bin/env bash

set -u

go=go
high=high
level=level
lead=lead
connector=connector
initial=G
suffix=HL
pattern="${go}${high}${level}|${high}${level}|${lead}${connector}|(^|[^a-zA-Z0-9])${initial}${suffix}([^a-zA-Z0-9]|$)"

check_tree() {
  local root status
  if ! root=$(git rev-parse --show-toplevel 2>/dev/null); then
    echo "::error::White-label check could not resolve a Git worktree." >&2
    return 2
  fi

  (
    cd "$root" || exit 2
    git grep -n -i -E -- "$pattern" -- \
      . \
      ':(exclude,glob,top)**/node_modules/**' \
      ':(exclude,glob,top)**/dist/**'
  )
  status=$?

  case "$status" in
    0)
      echo "::error::Vendor-name leakage detected. The public repo cannot reference the underlying CRM by name." >&2
      return 1
      ;;
    1)
      echo "OK — no vendor-name references found."
      return 0
      ;;
    *)
      echo "::error::White-label scan failed with git grep status $status." >&2
      return 2
      ;;
  esac
}

self_test() {
  local script tmp status git_index
  script=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/white-label-check.XXXXXX") || return 2
  trap 'rm -rf "$tmp"' RETURN

  mkdir -p "$tmp/clean/scripts"
  git -C "$tmp/clean" init -q || return 2
  cp "$script" "$tmp/clean/scripts/check-white-label.sh" || return 2
  printf '%s\n' 'clean public copy' > "$tmp/clean/README.md"
  git -C "$tmp/clean" add README.md scripts/check-white-label.sh || return 2

  if ! (cd "$tmp/clean" && bash scripts/check-white-label.sh >/dev/null); then
    echo "::error::White-label self-test clean case failed." >&2
    return 2
  fi

  printf '%s%s\n' "$lead" "$connector" > "$tmp/clean/leak.txt"
  git -C "$tmp/clean" add leak.txt || return 2
  (cd "$tmp/clean" && bash scripts/check-white-label.sh >/dev/null 2>&1)
  status=$?
  if [[ "$status" -ne 1 ]]; then
    echo "::error::White-label self-test leak case returned $status, expected 1." >&2
    return 2
  fi

  git_index="$tmp/clean/.git/index"
  mv "$git_index" "$git_index.saved" || return 2
  printf '%s\n' 'corrupt index' > "$git_index"
  (cd "$tmp/clean" && bash scripts/check-white-label.sh >/dev/null 2>&1)
  status=$?
  mv "$git_index.saved" "$git_index" || return 2
  if [[ "$status" -ne 2 ]]; then
    echo "::error::White-label self-test error case returned $status, expected 2." >&2
    return 2
  fi

  echo "OK — white-label guard clean, leak, and error cases passed."
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
else
  check_tree
fi
