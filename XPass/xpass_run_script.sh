#!/bin/bash


TYPE="multicast" #Only this you need to change!
CODEC="H264" # Change to H265 / H264

if [ "$TYPE" = "unicast" ]; then
  HOST="10.0.0.48"
elif [ "$TYPE" = "multicast" ]; then
  HOST="239.0.0.1"
fi
if [ "$CODEC" = "H264" ]; then
  VIDEO_FILE="../assets/test_videos/ARIG_in.ts"
elif [ "$CODEC" = "H265" ]; then
  VIDEO_FILE="../assets/test_videos/H265_23.ts"
fi
PORT="5600"
PASS="1"
SESSION_NAME="codecChange"
MAIN_CONFIG="main_config.yaml"
MAIN_CONFIG_2="main_config_2nd_process.yaml"

pkill ssh
sleep 3s 
sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no -t neuronics@10.0.0.30 << ENDSSH

# Kill old session if exists
tmux kill-session -t $SESSION_NAME 2>/dev/null

# Start new tmux session (detached)
tmux new-session -d -s $SESSION_NAME

#Window 1: Kill
tmux new-window -t $SESSION_NAME:1 -n 'kill'
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S docker exec runner-deamon pkill -9 -f run.sh" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S pkill -9 -f python" C-m
tmux send-keys -t $SESSION_NAME:1 "echo \"$PASS\" | sudo -S pkill -9 -f docker" C-m


# Window 2: Set Config
tmux new-window -t $SESSION_NAME:2 -n 'setConfig'
tmux send-keys -t $SESSION_NAME:2 "cd Forcer/config" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i 's/codec:[[:space:]]*.*/codec: $CODEC/' $MAIN_CONFIG" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i '/ts_muxer_module:/,/^[^ ]/ { s/type:[[:space:]]*.*/type: $TYPE/; s/host:[[:space:]]*.*/host: $HOST/; s/port:[[:space:]]*.*/port: $PORT/; }' $MAIN_CONFIG" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i 's/codec:[[:space:]]*.*/codec: $CODEC/' $MAIN_CONFIG_2" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i '/ts_muxer_module:/,/^[^ ]/ { s/type:[[:space:]]*.*/type: $TYPE/; s/host:[[:space:]]*.*/host: $HOST/; s/port:[[:space:]]*.*/port: $PORT/; }' $MAIN_CONFIG_2" C-m

tmux send-keys -t $SESSION_NAME:2 "cd .." C-m
tmux send-keys -t $SESSION_NAME:2 "cd Stream_Tests_Module/config" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i '/video_player:/,/^[^ ]/ { s/host:[[:space:]]*.*/host: $HOST/; s/port:[[:space:]]*.*/port: $PORT/; s/codec:[[:space:]]*.*/codec: $CODEC/; }' stream_tests_config.yaml" C-m
tmux send-keys -t $SESSION_NAME:2 "sed -i 's|ts_file:[[:space:]]*\".*\"|ts_file: \"$VIDEO_FILE\"|' stream_tests_config.yaml" C-m

exit
ENDSSH
sleep 3s
./xpass_servers.sh

# H264 VIDEO: ARIG_in.ts
# H265 VIDEO: H265_23.ts