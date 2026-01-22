#!/usr/bin/env bash
set -e

SESSION="scheduler-local-run"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "Stopping tmux session '$SESSION'..."
  tmux kill-session -t "$SESSION"
  echo "✓ Session stopped."
else
  echo "No tmux session named '$SESSION' is running."
fi
