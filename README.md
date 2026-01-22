# Local Job Scheduler – Local Development Guide

This repository contains a local development setup for a **scheduler**, **workers**, and a **client**.
You can run the system either manually in separate terminals or via `tmux` (recommended).

---

## Build

Run this once, or whenever code changes:

```bash
mvn -DskipTests clean package
```

--- 

## Running Without tmux (Manual Terminals)

Open three separate terminals.

Terminal 1 – Scheduler
```bash
mvn -pl scheduler exec:java
```

Terminal 2 – Worker
```bash
mvn -pl worker exec:java
```

Terminal 3 – Client
```bash
mvn -pl client exec:java
```

## Running With tmux (Recommended)

Using tmux keeps everything organized and easy to restart.

Start all services
```bash
./run-local-tmux.sh
```

This starts a tmux session named:
```bash
scheduler-local-run
```

with separate windows for:

```bash
scheduler
worker-1
worker-2 (if configured)
```

Attach to the running session
```bash
tmux attach -t scheduler-local-run
```

List tmux windows
```bash
tmux list-windows
```

Stop everything
```bash
./stop-local.sh
```

This kills the tmux session and all running JVM processes inside it.

### TMUX Basics (Quick Reference)

When inside tmux, all commands begin with ```Ctrl-b ```.

| Action | Keys |
|--------|------|
| Next window | ```Ctrl-b n``` |
| Previous window | ```Ctrl-b p``` |
| List windows | ```Ctrl-b w``` |
| Detach (leave tmux running) | ```Ctrl-b d``` |
| Kill current window | ```Ctrl-b &``` |
| Kill entire session | ```Ctrl-b : → kill-session``` |

## Important Notes

All services run inside the scheduler-local-run tmux session

Output for each service appears in its own tmux window

Detaching from tmux does not stop the services

Killing the tmux session terminates all running JVMs

### Recommended Workflow

For a clean restart:

```bash
./stop-local.sh
./run-local-tmux.sh
tmux attach -t scheduler-local-run
```
