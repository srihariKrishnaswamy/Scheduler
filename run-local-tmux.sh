#!/usr/bin/env bash
set -e

SESSION="scheduler-local-run"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

tmux new-session -d -s $SESSION -c "$ROOT_DIR"

# -------------------
# Scheduler
# -------------------
tmux rename-window -t $SESSION:0 scheduler
tmux send-keys -t $SESSION:0 "mvn -pl scheduler exec:java" C-m

# -------------------
# Worker 1
# -------------------
tmux new-window -t $SESSION -n worker-1 -c "$ROOT_DIR"
tmux send-keys -t $SESSION:worker-1 "mvn -pl worker exec:java" C-m

# -------------------
# Worker 2
# -------------------
tmux new-window -t $SESSION -n worker-2 -c "$ROOT_DIR"
tmux send-keys -t $SESSION:worker-2 "mvn -pl worker exec:java" C-m

# Give scheduler + workers time to start
sleep 3

# -------------------
# Client (submit job)
# -------------------
tmux new-window -t $SESSION -n client -c "$ROOT_DIR"
tmux send-keys -t $SESSION:client "mvn -pl client exec:java" C-m

echo "tmux session '$SESSION' started."
echo "Attach with: tmux attach -t $SESSION"
