#!/bin/bash
#
# This script uses the GH CLI to wait for PR checks to complete.
# Note: This script would not be necessary if https://github.com/cli/cli/issues/463 is resolved.
#
# @param $1 - PR number or URL

if ! which gh &>/dev/null; then
  echo "✋ This script requires the GitHub CLI to run. Please install it and try again."
  exit 1
fi

printf "⏱ PR checks pending, checking every 5 seconds and waiting for them to finish..."
while gh pr checks "$1" | grep -qE "pending|no checks"; do
  printf "."
  sleep 5
done

echo

if gh pr checks "$1" | grep -q "fail"; then
  echo "❌ PR checks failed!"
  exit 1
fi

if gh pr checks "$1" | grep  -q "pass"; then
  echo "✅ PR checks passed!"
  exit 0
fi

echo "🤔 An unknown error occurred!"
gh pr checks "$1"
exit 1
