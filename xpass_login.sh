#!/bin/bash

PASS="1"
SESSION_NAME="mysession"
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no -X neuronics@10.0.0.30 << ENDSSH
sshpass -p "$PASS" ssh -tt neuronics@10.0.0.30 "tmux attach -t mysession"
# Kill old session if exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Start new tmux session (detached)
tmux new-session -d -s $SESSION_NAME

# Window 1: Docker commands
tmux new-window -t $SESSION_NAME:1 -n 'docker'
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S docker start license-deamon" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S docker start runner-deamon" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S docker exec -w /home/neuronics/protrack runner-deamon ./run.sh" C-m


# Window 2: X11 permissions
tmux new-window -t $SESSION_NAME:2 -n 'xhost'
tmux send-keys -t $SESSION_NAME:2 "xhost +" C-m
tmux send-keys -t $SESSION_NAME:2 "xhost +local:docker" C-m

# Window 3: Forcer commands
tmux new-window -t $SESSION_NAME:3 -n 'forcer'
tmux send-keys -t $SESSION_NAME:3 "cd Forcer" C-m
tmux send-keys -t $SESSION_NAME:3 "./start_and_enter.sh" C-m
tmux send-keys -t $SESSION_NAME:3 "./Stream_Tests_Module/test_servers.sh" C-m

# Window 4: Forcer commands
tmux new-window -t $SESSION_NAME:4 -n 'forcerMain'
tmux send-keys -t $SESSION_NAME:4 "cd Forcer" C-m
tmux send-keys -t $SESSION_NAME:4 "./start_and_enter.sh" C-m
tmux send-keys -t $SESSION_NAME:4 "python3 -m src.main" C-m

ENDSSH
sshpass -p "$PASS" ssh -tt -X neuronics@10.0.0.30 "tmux attach -t $SESSION_NAME"


#To see the terminals works you should write in the terminal:
# "CTRL+b 1" for window 1
# "CTRL+b 1" for window 2
# "CTRL+b 1" for window 3

#You can also go to each terminal and write CTRL+C and it will end the proccess.

