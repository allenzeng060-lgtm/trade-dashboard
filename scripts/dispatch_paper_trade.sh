#!/bin/bash
# Dispatch the paper-trade workflow. Intended for Mac launchd (every 5 min).
# Reads GITHUB_DASHBOARD_TOKEN or GITHUB_TOKEN from the environment or
# $HOME/.trade_watch/env. Never prints the token.
set -euo pipefail
cd "${HOME}/.trade_watch" 2>/dev/null || true
if [[ -f ./env ]]; then
  # shellcheck disable=SC1091
  source ./env
fi
TOKEN="${GITHUB_DASHBOARD_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -z "$TOKEN" ]]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') dispatch: no token in env" >> "${HOME}/.trade_watch/dispatch.log" 2>/dev/null || true
  exit 1
fi
MODE="${1:-auto}"
LOG="${HOME}/.trade_watch/dispatch.log"
mkdir -p "${HOME}/.trade_watch"
code=$(curl -sS -o /tmp/paper-dispatch.out -w "%{http_code}" \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -H "User-Agent: dante-mac-paper-dispatch/1.0" \
  "https://api.github.com/repos/allenzeng060-lgtm/trade-dashboard/actions/workflows/paper-trade.yml/dispatches" \
  -d "{\"ref\":\"main\",\"inputs\":{\"mode\":\"${MODE}\"}}")
# truncate log; never include the token
{
  echo "$(date '+%Y-%m-%d %H:%M:%S') dispatch mode=${MODE} http=${code}"
  if [[ "$code" != "204" ]]; then
    sed -E 's/github_pat_[A-Za-z0-9_]+/[redacted]/g; s/ghp_[A-Za-z0-9]+/[redacted]/g' /tmp/paper-dispatch.out | head -c 400
    echo
  fi
} >> "$LOG"
tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
rm -f /tmp/paper-dispatch.out
[[ "$code" == "204" ]]
