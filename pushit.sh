#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

usage() {
  cat <<'EOF'
Usage:
  ./pushit.sh [message]
  ./pushit.sh --all [message]

Behavior:
  - Stages and commits changes, rebases on origin, then pushes.
  - Default staging is tracked-only (safe): `git add -u`
  - Use --all to include untracked/new files too: `git add -A`

Examples:
  ./pushit.sh "Fix leaflet marker"
  ./pushit.sh --all "Add new dataset"
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

stage_mode="tracked"
if [[ "${1:-}" == "--all" ]]; then
  stage_mode="all"
  shift
fi

msg="${*:-}"  # remaining args as message
if [[ -z "$msg" ]]; then
  msg="Update $(date -Iseconds)"
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: Not inside a git repository." >&2
  exit 1
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" == "HEAD" ]]; then
  echo "ERROR: Detached HEAD; checkout a branch first." >&2
  exit 1
fi

# Guard against mid-rebase state.
if [[ -d .git/rebase-merge || -d .git/rebase-apply ]]; then
  echo "ERROR: Rebase in progress. Resolve it first, then rerun." >&2
  exit 1
fi

# Stage changes.
if [[ "$stage_mode" == "all" ]]; then
  git add -A
else
  git add -u
fi

# Commit only if we have staged changes.
if ! git diff --cached --quiet; then
  git commit -m "$msg"
else
  echo "No staged changes to commit. (Tip: use --all to include untracked files.)"
fi

# Rebase on latest upstream, then push.
# (Pull after commit avoids 'cannot pull with rebase: unstaged changes' failures.)
git pull --rebase origin "$branch"
git push origin HEAD

echo "OK: pushed $(git rev-parse --short HEAD) to origin/$branch"
