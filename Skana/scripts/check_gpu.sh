#!/usr/bin/env bash
# --------------------------------------------------------------------------
# check_gpu.sh — Verify that NVIDIA GPU + GStreamer NVDEC/NVENC are available
# --------------------------------------------------------------------------
set -euo pipefail

echo "=== NVIDIA Driver ==="
if ! nvidia-smi; then
    echo "FAIL: nvidia-smi not found.  Install the NVIDIA driver."
    exit 1
fi

echo ""
echo "=== GStreamer NVDEC (H.265 decoder) ==="
if gst-inspect-1.0 nvh265dec >/dev/null 2>&1; then
    echo "OK: nvh265dec"
elif gst-inspect-1.0 nvv4l2decoder >/dev/null 2>&1; then
    echo "OK: nvv4l2decoder (Jetson)"
else
    echo "FAIL: No hardware H.265 decoder found."
    echo "      Install gst-plugins-bad built with nvcodec / CUDA support."
    exit 1
fi

echo ""
echo "=== GStreamer NVENC (H.264 encoder) ==="
if gst-inspect-1.0 nvh264enc >/dev/null 2>&1; then
    echo "OK: nvh264enc"
elif gst-inspect-1.0 nvv4l2h264enc >/dev/null 2>&1; then
    echo "OK: nvv4l2h264enc (Jetson)"
else
    echo "FAIL: No hardware H.264 encoder found."
    echo "      Install gst-plugins-bad built with nvcodec / CUDA support."
    exit 1
fi

echo ""
echo "=== GStreamer webrtcbin ==="
if ! gst-inspect-1.0 webrtcbin >/dev/null 2>&1; then
    echo "FAIL: webrtcbin not found."
    echo "      Install gstreamer1.0-nice and gstreamer1.0-plugins-bad."
    exit 1
fi
echo "OK: webrtcbin"

echo ""
echo "All GPU / GStreamer checks passed."
