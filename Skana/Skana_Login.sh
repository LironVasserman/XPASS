#!/bin/bash


SUDO_PASS="2912"
SESSION_PASS="1"
SESSION_NAME="skanna"
CONFIG="config.yaml"
HOST="192.168.1.70"


sshpass -p "$SESSION_PASS" ssh -o StrictHostKeyChecking=no -X neuronics@192.168.1.60 << ENDSSH

# Kill old session if exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Start new tmux session (detached)
tmux new-session -d -s $SESSION_NAME

# Window 1: Set Config
tmux new-window -t $SESSION_NAME:1 -n 'setConfig'
tmux send-keys -t $SESSION_NAME:1 "cd Documents/v0.9" C-m
tmux send-keys -t $SESSION_NAME:1 "sed -i 's/rtp_host:[[:space:]]*.*/rtp_host: $HOST/' $CONFIG" C-m

# Window 2: Run Docker
tmux new-window -t $SESSION_NAME:2 -n 'forcerMain'
tmux send-keys -t $SESSION_NAME:2 "cd Documents/v0.9" C-m
tmux send-keys -t $SESSION_NAME:2 "./run.sh" C-m



ENDSSH
sshpass -p "$SESSION_PASS" ssh -tt -X neuronics@192.168.1.60 "tmux attach -t $SESSION_NAME"

#To do: dont forget to write a script that kill the process!

# how to run the index.html file: 
#xdg-open /home/neuronics/Testing/Skana/index.html

