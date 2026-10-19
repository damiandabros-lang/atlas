#!/usr/bin/env bash
# Publish ATLAS to GitHub: rewrites the image-repository placeholder to your login and creates
# the public repository with `gh`. Usage: ./scripts/publish-github.sh <github-login> [repo-name]
set -euo pipefail
LOGIN="${1:?usage: $0 <github-login> [repo-name]}"
REPO="${2:-atlas}"
cd "$(dirname "$0")/.."
command -v gh >/dev/null || { echo "GitHub CLI (gh) not installed: brew install gh && gh auth login"; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "run: gh auth login"; exit 1; }

if [ "$LOGIN" != "ddabros" ] || [ "$REPO" != "atlas" ]; then
  grep -rl "ghcr.io/ddabros/atlas" --exclude-dir=.git --exclude-dir=.venv . \
    | xargs sed -i '' "s#ghcr.io/ddabros/atlas#ghcr.io/${LOGIN}/${REPO}#g"
  grep -rl "github.com/ddabros/atlas" --exclude-dir=.git --exclude-dir=.venv . \
    | xargs sed -i '' "s#github.com/ddabros/atlas#github.com/${LOGIN}/${REPO}#g" || true
  git add -A && git commit -q -m "chore: point image/repo references at ${LOGIN}/${REPO}" || true
fi

gh repo create "${LOGIN}/${REPO}" --public --source=. --remote=origin --push \
  --description "ATLAS — Network Service Orchestration Platform: MPLS L3VPN provisioning on Temporal (FastAPI, PostgreSQL, Saga compensation, reconciliation, Helm)"
echo
echo "Published: https://github.com/${LOGIN}/${REPO}   (CI: Actions tab; image: ghcr.io/${LOGIN}/${REPO} after first main build)"
