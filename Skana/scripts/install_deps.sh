#!/usr/bin/env bash
# --------------------------------------------------------------------------
# install_deps.sh — Install system + Python dependencies for teleop_client
#
# Targets: Ubuntu 22.04 / 24.04 with NVIDIA GPU + CUDA drivers pre-installed.
# --------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== Installing system packages ==="
sudo apt-get update
sudo apt-get install -y \
    python3-pip python3-venv \
    python3-gi \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-nice \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    gir1.2-gst-plugins-base-1.0 \
    gir1.2-gst-plugins-bad-1.0 \
    gir1.2-gstreamer-1.0

echo ""
echo "=== Checking for NVIDIA GStreamer plugins ==="
gst-inspect-1.0 nvh265dec 2>/dev/null && echo "  OK: nvh265dec found" || \
gst-inspect-1.0 nvv4l2decoder 2>/dev/null && echo "  OK: nvv4l2decoder found" || \
echo "  WARNING: No NVIDIA H.265 decoder plugin found."

gst-inspect-1.0 nvh264enc 2>/dev/null && echo "  OK: nvh264enc found" || \
gst-inspect-1.0 nvv4l2h264enc 2>/dev/null && echo "  OK: nvv4l2h264enc found" || \
echo "  WARNING: No NVIDIA H.264 encoder plugin found."

echo ""
echo "=== Creating Python virtual environment ==="
cd "$PROJECT_ROOT"
python3 -m venv .venv --system-site-packages
# shellcheck disable=SC1091
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "=== Done ==="
echo "Activate the venv with:  source .venv/bin/activate"
