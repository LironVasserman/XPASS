#!/bin/bash

PASS="1"
SESSION_NAME="mysessionkill"
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no -X neuronics@10.0.0.30 << ENDSSH

# Kill old session if exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Start new tmux session (detached)
tmux new-session -d -s $SESSION_NAME

tmux new-window -t $SESSION_NAME:1 -n 'kill'
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S docker exec runner-deamon pkill -9 -f run.sh" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S pkill -9 -f python" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S pkill -9 -f docker" C-m

exit
ENDSSH

