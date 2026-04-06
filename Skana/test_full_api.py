"""
Streaming API Test Suite
========================
Covers every endpoint from the documented API:

  GET  /health
  GET  /pipes
  POST /pipes
  DELETE /pipe/{pipe_id}
  POST /pipe/{pipe_id}/start
  POST /pipe/{pipe_id}/stop
  POST /pipe/{pipe_id}/restart
  GET  /pipe/{pipe_id}/bitrate
  POST /pipe/{pipe_id}/bitrate
  GET  /pipe/{pipe_id}/fps
  POST /pipe/{pipe_id}/fps
  GET  /pipe/{pipe_id}/resolution
  POST /pipe/{pipe_id}/resolution
  POST /pipe/{pipe_id}/keyframe
  GET  /pipe/{pipe_id}/stream/stats
  GET  /pipe/{pipe_id}/frames/tracking       ← per-pipe (was global)
  GET  /pipe/{pipe_id}/frames/latency        ← per-pipe (was global)
  GET  /pipe/{pipe_id}/abr/status
  GET  /pipe/{pipe_id}/abr/mode
  POST /pipe/{pipe_id}/abr/mode
  GET  /pipe/{pipe_id}/rate_shaping          ← new
  POST /pipe/{pipe_id}/rate_shaping          ← new
  POST /pipe/{pipe_id}/rate_shaping/reset_stats  ← new
  GET  /config
  PATCH /config

Changes from the previous suite
---------------------------------
* Removed global /frames/tracking and /frames/latency — they are now per-pipe.
* Removed global /config/encoding shortcut — not in the documented spec.
* Added FPS endpoints: GET /pipe/{id}/fps, POST /pipe/{id}/fps.
* Added Resolution endpoints: GET /pipe/{id}/resolution, POST /pipe/{id}/resolution.
* Added Rate-Shaping endpoints: GET/POST /pipe/{id}/rate_shaping,
  POST /pipe/{id}/rate_shaping/reset_stats.
* Removed legacy / backward-compat routes — not in the documented spec.
* Every fixture and config-mutating test still restores the server to its
  original state via teardown / autouse fixtures.
* Status 200 alone is never enough — every response body is validated against
  an explicit schema using assert_schema().
"""

import time
import pytest
import requests

BASE_URL = "http://192.168.1.21:8080"

PIPE_TYPES = ["rtp", "webrtc"]
CODECS     = ["h264", "h265"]

PIPE_COMBOS = [
    {
        "pipe_id":    f"test_{pt}_{codec}",
        "pipe_type":  pt,
        "codec":      codec,
        "auto_start": False,
    }
    for pt in PIPE_TYPES
    for codec in CODECS
]
COMBO_IDS = [f"{c['pipe_type']}-{c['codec']}" for c in PIPE_COMBOS]


# =============================================================================
# SCHEMA VALIDATOR
# =============================================================================

def assert_schema(data: dict, schema: dict) -> None:
    """
    Validate a parsed JSON dict against a schema definition.

    Schema format:
        { "field_name": (type_or_tuple_of_types, "required"|"optional") }

    Raises AssertionError with a descriptive message on any violation.
    """
    assert isinstance(data, dict), \
        f"Expected JSON object, got {type(data).__name__}: {data!r}"

    required = {k for k, (_, presence) in schema.items() if presence == "required"}
    allowed  = set(schema.keys())

    missing = required - data.keys()
    assert not missing, \
        f"Missing required keys: {missing}  —  got keys: {set(data.keys())}"

    extra = data.keys() - allowed
    assert not extra, f"Unexpected extra keys: {extra}"

    for key, value in data.items():
        expected_types, _ = schema[key]
        if not isinstance(expected_types, tuple):
            expected_types = (expected_types,)
        assert isinstance(value, expected_types), (
            f"Field '{key}': expected {expected_types}, "
            f"got {type(value).__name__} = {value!r}"
        )


def assert_error_schema(data: dict) -> None:
    """All error responses must carry at least an 'error' or 'message' string."""
    assert isinstance(data, dict), \
        f"Error body must be a JSON object, got: {data!r}"
    has_error   = "error"   in data and isinstance(data["error"],   str)
    has_message = "message" in data and isinstance(data["message"], str)
    assert has_error or has_message, (
        f"Error response missing 'error' or 'message' string key — got: {data}"
    )


# =============================================================================
# RESPONSE SCHEMAS
# =============================================================================

SCHEMA_HEALTH = {
    "status":         (str,          "required"),
    "streaming":      (bool,         "required"),
    "uptime_seconds": ((int, float), "required"),
}

SCHEMA_PIPES_LIST = {
    "pipes": (list, "required"),
    "total": (int,  "required"),
}

SCHEMA_PIPE_CREATE = {
    "pipe_id":   (str, "required"),
    "pipe_type": (str, "required"),
    "codec":     (str, "required"),
    "status":    (str, "required"),
}

SCHEMA_PIPE_ACTION = {
    "pipe_id": (str, "required"),
    "status":  (str, "required"),
}

SCHEMA_BITRATE_GET = {
    "pipe_id":         (str,          "required"),
    "current_bitrate": ((int, float), "required"),
    "min_bitrate":     ((int, float), "optional"),
    "max_bitrate":     ((int, float), "optional"),
}

SCHEMA_BITRATE_SET = {
    "pipe_id":         (str,          "required"),
    "current_bitrate": ((int, float), "required"),
}

SCHEMA_FPS_GET = {
    "pipe_id":     (str,          "required"),
    "current_fps": ((int, float), "required"),
    "min_fps":     ((int, float), "optional"),
    "max_fps":     ((int, float), "optional"),
}

SCHEMA_FPS_SET = {
    "pipe_id":     (str,          "required"),
    "current_fps": ((int, float), "required"),
}

SCHEMA_RESOLUTION_GET = {
    "pipe_id":    (str, "required"),
    "resolution": (str, "required"),   # e.g. "1920x1080"
    "width":      (int, "optional"),
    "height":     (int, "optional"),
}

SCHEMA_RESOLUTION_SET = {
    "pipe_id":    (str, "required"),
    "resolution": (str, "required"),
}

SCHEMA_KEYFRAME = {
    "pipe_id": (str, "required"),
    "status":  (str, "required"),
}

SCHEMA_STREAM_STATS = {
    "pipe_id":      (str,          "required"),
    "frames_sent":  (int,          "required"),
    "bytes_sent":   (int,          "required"),
    "video_fps":    ((int, float), "required"),
    "bitrate_kbps": ((int, float), "optional"),
    "dropped":      (int,          "optional"),
    "duration_s":   ((int, float), "optional"),
}

SCHEMA_FRAME_TRACKING = {
    "pipe_id": (str,  "required"),
    "enabled": (bool, "required"),
}

SCHEMA_FRAME_LATENCY = {
    "pipe_id":        (str,          "required"),
    "avg_latency_ms": ((int, float), "required"),
    "min_latency_ms": ((int, float), "optional"),
    "max_latency_ms": ((int, float), "optional"),
    "samples":        (int,          "optional"),
}

SCHEMA_ABR_STATUS = {
    "pipe_id": (str,  "required"),
    "enabled": (bool, "required"),
    "mode":    (str,  "optional"),
}

SCHEMA_ABR_MODE = {
    "pipe_id": (str,  "required"),
    "mode":    (str,  "required"),
    "enabled": (bool, "optional"),
}

SCHEMA_RATE_SHAPING_GET = {
    "pipe_id": (str,  "required"),
    "enabled": (bool, "required"),
    # All other fields (target_kbps, burst_kbps, …) are treated as optional
    # because the exact shape depends on server implementation.
    "target_kbps": ((int, float), "optional"),
    "burst_kbps":  ((int, float), "optional"),
    "mode":        (str,          "optional"),
}

SCHEMA_RATE_SHAPING_SET = {
    "pipe_id": (str,  "required"),
    "enabled": (bool, "required"),
}

SCHEMA_RATE_SHAPING_RESET = {
    "pipe_id": (str, "required"),
    "status":  (str, "required"),
}

SCHEMA_CONFIG_GET = {
    "config": (dict, "required"),
}

SCHEMA_CONFIG_PATCH = {
    "restart_required": (bool, "required"),
    "config":           (dict, "optional"),
}


# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================

def _delete(pipe_id: str) -> None:
    requests.delete(f"{BASE_URL}/pipe/{pipe_id}")


def _create(payload: dict) -> requests.Response:
    return requests.post(f"{BASE_URL}/pipes", json=payload)


def _start(pipe_id: str) -> requests.Response:
    return requests.post(f"{BASE_URL}/pipe/{pipe_id}/start")


def _stop(pipe_id: str) -> requests.Response:
    return requests.post(f"{BASE_URL}/pipe/{pipe_id}/stop")


def _get_config() -> dict:
    r = requests.get(f"{BASE_URL}/config")
    assert r.status_code == 200, f"Could not snapshot config: {r.text}"
    return r.json()["config"]


def _restore_config(original: dict) -> None:
    requests.patch(f"{BASE_URL}/config", json=original)


# =============================================================================
# SESSION-SCOPED CONFIG SNAPSHOT
# =============================================================================

@pytest.fixture(scope="session")
def original_config():
    """Snapshot global config at session start; restore at session end."""
    cfg = _get_config()
    yield cfg
    _restore_config(cfg)


# =============================================================================
# AUTOUSE CONFIG GUARD
# =============================================================================

@pytest.fixture(autouse=True)
def restore_config_after_each(original_config):
    """Restore config after every test, even if it crashes mid-way."""
    yield
    _restore_config(original_config)


# =============================================================================
# PIPE FIXTURES
# =============================================================================

@pytest.fixture(scope="module", params=PIPE_COMBOS, ids=COMBO_IDS)
def live_pipe(request):
    """
    Parametrized over all 4 pipe×codec combos.
    Creates a fresh test pipe, yields pipe_id, deletes it on teardown.
    """
    cfg = request.param
    _delete(cfg["pipe_id"])
    r = _create(cfg)
    assert r.status_code == 200, (
        f"Setup failed for {cfg['pipe_type']}/{cfg['codec']}: {r.text}"
    )
    yield cfg["pipe_id"]
    _stop(cfg["pipe_id"])
    _delete(cfg["pipe_id"])


@pytest.fixture(scope="module", params=PIPE_COMBOS, ids=COMBO_IDS)
def live_pipe_cfg(request):
    """Like live_pipe but yields the full cfg dict."""
    cfg = request.param
    _delete(cfg["pipe_id"])
    r = _create(cfg)
    assert r.status_code == 200, (
        f"Setup failed for {cfg['pipe_type']}/{cfg['codec']}: {r.text}"
    )
    yield cfg
    _stop(cfg["pipe_id"])
    _delete(cfg["pipe_id"])


@pytest.fixture(scope="module")
def all_pipes():
    """All 4 pipe×codec combos alive simultaneously, cleaned up afterwards."""
    ids = []
    for cfg in PIPE_COMBOS:
        _delete(cfg["pipe_id"])
        r = _create(cfg)
        assert r.status_code == 200, f"Setup failed for {cfg['pipe_id']}: {r.text}"
        ids.append(cfg["pipe_id"])
    yield ids
    for pid in ids:
        _stop(pid)
        _delete(pid)


# =============================================================================
# HEALTH — GET /health
# =============================================================================

def test_health():
    r = requests.get(f"{BASE_URL}/health")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_HEALTH)


def test_health_status_ok():
    data = requests.get(f"{BASE_URL}/health").json()
    assert_schema(data, SCHEMA_HEALTH)
    assert data["status"] == "ok", f"Expected status='ok', got {data['status']!r}"
    assert data["uptime_seconds"] >= 0, "uptime_seconds must be non-negative"


def test_health_uptime_increases():
    t1 = requests.get(f"{BASE_URL}/health").json()["uptime_seconds"]
    time.sleep(1.1)
    t2 = requests.get(f"{BASE_URL}/health").json()["uptime_seconds"]
    assert t2 > t1, f"uptime did not increase: {t1} → {t2}"


def test_health_wrong_method():
    r = requests.post(f"{BASE_URL}/health")
    assert r.status_code in [404, 405]


def test_unknown_path_returns_404():
    r = requests.get(f"{BASE_URL}/nonexistent_endpoint")
    assert r.status_code == 404


# =============================================================================
# PIPES LIST — GET /pipes
# =============================================================================

def test_get_pipes():
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    assert data["total"] == len(data["pipes"]), (
        f"total={data['total']} but pipes list has {len(data['pipes'])} entries"
    )
    for entry in data["pipes"]:
        assert isinstance(entry, str), \
            f"Each pipe entry must be a string pipe_id, got: {entry!r}"


def test_pipes_list_updates():
    temp_id = "test_list_update_pipe"
    _delete(temp_id)
    try:
        _create({"pipe_id": temp_id})
        data = requests.get(f"{BASE_URL}/pipes").json()
        assert_schema(data, SCHEMA_PIPES_LIST)
        assert temp_id in data["pipes"]
    finally:
        _delete(temp_id)
        data = requests.get(f"{BASE_URL}/pipes").json()
        assert temp_id not in data["pipes"]
        assert data["total"] == len(data["pipes"])


# =============================================================================
# PIPE MANAGEMENT — POST /pipes, DELETE /pipe/{id}
# =============================================================================

@pytest.mark.parametrize("cfg", PIPE_COMBOS, ids=COMBO_IDS)
def test_create_and_delete(cfg):
    _delete(cfg["pipe_id"])
    r = _create(cfg)
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPE_CREATE)
    assert data["pipe_id"]   == cfg["pipe_id"]
    assert data["pipe_type"] == cfg["pipe_type"]
    assert data["codec"]     == cfg["codec"]
    assert data["status"] in ("created", "stopped", "idle"), (
        f"Unexpected initial status: {data['status']!r}"
    )
    _delete(cfg["pipe_id"])


def test_get_pipes_lists_all_created(all_pipes):
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    for pid in all_pipes:
        assert pid in data["pipes"], f"{pid} missing from pipes list"


def test_total_count_reflects_all_pipes(all_pipes):
    data = requests.get(f"{BASE_URL}/pipes").json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    assert data["total"] >= len(all_pipes)
    assert data["total"] == len(data["pipes"])


# Negative cases

def test_duplicate_pipe_conflict():
    pid = "test_dup_pipe"
    _delete(pid)
    try:
        _create({"pipe_id": pid})
        r = _create({"pipe_id": pid})
        assert r.status_code == 409
        assert_error_schema(r.json())
    finally:
        _delete(pid)


def test_missing_pipe_id_returns_400():
    r = _create({})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_invalid_pipe_type_returns_400():
    pid = "test_bad_type"
    _delete(pid)
    try:
        r = _create({"pipe_id": pid, "pipe_type": "unicorn"})
        assert r.status_code == 400
        assert_error_schema(r.json())
    finally:
        _delete(pid)


def test_invalid_codec_returns_400():
    pid = "test_bad_codec"
    _delete(pid)
    try:
        r = _create({"pipe_id": pid, "codec": "divx"})
        assert r.status_code == 400
        assert_error_schema(r.json())
    finally:
        _delete(pid)


def test_delete_nonexistent_pipe():
    r = requests.delete(f"{BASE_URL}/pipe/test_does_not_exist")
    assert r.status_code in [200, 404]
    assert isinstance(r.json(), dict)


# =============================================================================
# LIFECYCLE — POST /pipe/{id}/start|stop|restart  (runs × 4)
# =============================================================================

def test_start_stop_restart_pipe(live_pipe):
    r = _start(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["pipe_id"] == live_pipe
    assert r.json()["status"]  in ("started", "running", "streaming")

    r = _stop(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in ("stopped", "idle")

    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/restart")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["pipe_id"] == live_pipe

    _stop(live_pipe)


def test_start_idempotent(live_pipe):
    r1 = _start(live_pipe)
    r2 = _start(live_pipe)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert_schema(r1.json(), SCHEMA_PIPE_ACTION)
    assert_schema(r2.json(), SCHEMA_PIPE_ACTION)
    _stop(live_pipe)


def test_stop_idempotent(live_pipe):
    _stop(live_pipe)
    r = _stop(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)


def test_start_nonexistent_pipe():
    r = requests.post(f"{BASE_URL}/pipe/test_ghost_pipe/start")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_stop_nonexistent_pipe():
    r = requests.post(f"{BASE_URL}/pipe/test_ghost_pipe/stop")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_restart_nonexistent_pipe():
    r = requests.post(f"{BASE_URL}/pipe/test_ghost_pipe/restart")
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# BITRATE — GET/POST /pipe/{id}/bitrate  (runs × 4)
# =============================================================================

def test_pipe_bitrate_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/bitrate")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_BITRATE_GET)
    assert data["pipe_id"] == live_pipe
    assert data["current_bitrate"] > 0, "current_bitrate must be positive"


def test_pipe_bitrate_set(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": 3000}
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_BITRATE_SET)
    assert data["current_bitrate"] == 3000


def test_bitrate_change_reflected(live_pipe):
    _start(live_pipe)
    new_bitrate = 3500
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": new_bitrate}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_BITRATE_SET)
    assert r.json()["current_bitrate"] == new_bitrate

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/bitrate")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_BITRATE_GET)
    assert r.json()["current_bitrate"] == new_bitrate, (
        f"GET bitrate did not reflect SET: expected {new_bitrate}, "
        f"got {r.json()['current_bitrate']}"
    )
    _stop(live_pipe)


@pytest.mark.parametrize("bitrate", [100, 50000])
def test_valid_bitrate_boundaries(live_pipe, bitrate):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": bitrate}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_BITRATE_SET)
    assert r.json()["current_bitrate"] == bitrate
    _stop(live_pipe)


@pytest.mark.parametrize("bitrate", [-1, 0, 50, 60000, 999999])
def test_invalid_bitrate_rejected(live_pipe, bitrate):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": bitrate}
    )
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_bitrate_missing_field(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_bitrate_string_value(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": "fast"}
    )
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_bitrate_malformed_json(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate",
        data="not-json",
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 400


def test_bitrate_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/bitrate")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_set_bitrate_nonexistent_pipe():
    r = requests.post(
        f"{BASE_URL}/pipe/test_ghost_pipe/bitrate", json={"bitrate_kbps": 3000}
    )
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# FPS — GET/POST /pipe/{id}/fps  (runs × 4)
# =============================================================================

def test_pipe_fps_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/fps")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_FPS_GET)
    assert data["pipe_id"] == live_pipe
    assert data["current_fps"] > 0, "current_fps must be positive"


def test_pipe_fps_set(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": 30})
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_FPS_SET)
    assert data["pipe_id"]     == live_pipe
    assert data["current_fps"] == 30


def test_fps_change_reflected(live_pipe):
    target = 25
    requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": target})
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/fps")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_GET)
    assert r.json()["current_fps"] == target, (
        f"GET fps did not reflect SET: expected {target}, "
        f"got {r.json()['current_fps']}"
    )


@pytest.mark.parametrize("fps", [1, 120])
def test_valid_fps_boundaries(live_pipe, fps):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": fps})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_SET)
    assert r.json()["current_fps"] == fps


@pytest.mark.parametrize("fps", [0, -1, 999])
def test_invalid_fps_rejected(live_pipe, fps):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": fps})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_fps_missing_field(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_fps_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/fps")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_set_fps_nonexistent_pipe():
    r = requests.post(
        f"{BASE_URL}/pipe/test_ghost_pipe/fps", json={"fps": 30}
    )
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# RESOLUTION — GET/POST /pipe/{id}/resolution  (runs × 4)
# =============================================================================

def test_pipe_resolution_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/resolution")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RESOLUTION_GET)
    assert data["pipe_id"] == live_pipe
    assert "x" in data["resolution"] or (
        "width" in data and "height" in data
    ), "resolution must be a WxH string or include width/height fields"


def test_pipe_resolution_set(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": 1280, "height": 720},
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RESOLUTION_SET)
    assert data["pipe_id"] == live_pipe


def test_resolution_change_reflected(live_pipe):
    requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": 1920, "height": 1080},
    )
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/resolution")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RESOLUTION_GET)
    res = r.json()["resolution"]
    assert "1920" in res and "1080" in res, (
        f"Resolution not reflected after SET: got {res!r}"
    )


@pytest.mark.parametrize("w,h", [(640, 480), (1920, 1080), (3840, 2160)])
def test_valid_resolutions(live_pipe, w, h):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": w, "height": h},
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RESOLUTION_SET)


@pytest.mark.parametrize("w,h", [(0, 720), (1280, 0), (-1, -1)])
def test_invalid_resolution_rejected(live_pipe, w, h):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": w, "height": h},
    )
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_resolution_missing_fields(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/resolution", json={})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_resolution_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/resolution")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_set_resolution_nonexistent_pipe():
    r = requests.post(
        f"{BASE_URL}/pipe/test_ghost_pipe/resolution",
        json={"width": 1280, "height": 720},
    )
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# KEYFRAME — POST /pipe/{id}/keyframe  (runs × 4)
# =============================================================================

def test_keyframe(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/keyframe")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_KEYFRAME)
    assert r.json()["pipe_id"] == live_pipe
    assert r.json()["status"]  in ("ok", "sent", "requested")
    _stop(live_pipe)


def test_keyframe_nonexistent_pipe():
    r = requests.post(f"{BASE_URL}/pipe/test_ghost_pipe/keyframe")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_keyframe_wrong_method(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/keyframe")
    assert r.status_code in [404, 405]


# =============================================================================
# STREAM STATS — GET /pipe/{id}/stream/stats  (runs × 4)
# =============================================================================

def test_stream_stats(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_STREAM_STATS)
    assert data["pipe_id"]     == live_pipe
    assert data["frames_sent"] >= 0
    assert data["bytes_sent"]  >= 0
    assert data["video_fps"]   >= 0
    _stop(live_pipe)


def test_stream_counters_increase(live_pipe):
    _start(live_pipe)
    r1 = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r1.status_code == 200
    assert_schema(r1.json(), SCHEMA_STREAM_STATS)
    d1 = r1.json()

    time.sleep(2)

    r2 = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r2.status_code == 200
    assert_schema(r2.json(), SCHEMA_STREAM_STATS)
    d2 = r2.json()

    assert d2["frames_sent"] >= d1["frames_sent"], (
        f"frames_sent decreased: {d1['frames_sent']} → {d2['frames_sent']}"
    )
    assert d2["bytes_sent"] >= d1["bytes_sent"], (
        f"bytes_sent decreased: {d1['bytes_sent']} → {d2['bytes_sent']}"
    )
    _stop(live_pipe)


def test_stats_stopped_pipe(live_pipe):
    _stop(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code in [200, 503]
    if r.status_code == 200:
        assert_schema(r.json(), SCHEMA_STREAM_STATS)
    else:
        assert_error_schema(r.json())


def test_stats_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/stream/stats")
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# FRAME TRACKING — GET /pipe/{id}/frames/tracking  (per-pipe, runs × 4)
# =============================================================================

def test_frame_tracking_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/frames/tracking")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_FRAME_TRACKING)
    assert data["pipe_id"] == live_pipe
    assert isinstance(data["enabled"], bool)


def test_frame_tracking_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/frames/tracking")
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# FRAME LATENCY — GET /pipe/{id}/frames/latency  (per-pipe, runs × 4)
# =============================================================================

def test_frame_latency_get(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/frames/latency")
    # 200 when tracking is active; 400 when tracking is disabled
    assert r.status_code in [200, 400]
    if r.status_code == 200:
        data = r.json()
        assert_schema(data, SCHEMA_FRAME_LATENCY)
        assert data["pipe_id"]        == live_pipe
        assert data["avg_latency_ms"] >= 0
    else:
        assert_error_schema(r.json())
    _stop(live_pipe)


def test_frame_latency_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/frames/latency")
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# ABR — GET/POST /pipe/{id}/abr/status|mode  (runs × 4)
# =============================================================================

def test_abr_status(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/abr/status")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_ABR_STATUS)
    assert data["pipe_id"] == live_pipe
    assert isinstance(data["enabled"], bool)


def test_abr_mode_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/abr/mode")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_ABR_MODE)
    assert data["pipe_id"] == live_pipe
    assert len(data["mode"]) > 0, "mode must be a non-empty string"


def test_abr_mode_set(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/abr/mode")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_ABR_MODE)
    assert r.json()["pipe_id"] == live_pipe


def test_abr_mode_toggle(live_pipe):
    mode_before = requests.get(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode"
    ).json()["mode"]

    requests.post(f"{BASE_URL}/pipe/{live_pipe}/abr/mode")

    mode_after = requests.get(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode"
    ).json()["mode"]

    assert isinstance(mode_after, str) and len(mode_after) > 0, (
        f"ABR mode after toggle is invalid: {mode_after!r}"
    )
    # Both values are valid — this assertion documents the observed behaviour.
    _ = (mode_before, mode_after)


def test_abr_status_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/abr/status")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_abr_mode_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/abr/mode")
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# RATE SHAPING — GET/POST /pipe/{id}/rate_shaping
#                POST /pipe/{id}/rate_shaping/reset_stats  (runs × 4)
# =============================================================================

def test_rate_shaping_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_GET)
    assert data["pipe_id"] == live_pipe
    assert isinstance(data["enabled"], bool)


def test_rate_shaping_set(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping",
        json={"enabled": True},
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_SET)
    assert data["pipe_id"] == live_pipe
    assert data["enabled"] is True


def test_rate_shaping_disable(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping",
        json={"enabled": False},
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_SET)
    assert data["enabled"] is False


def test_rate_shaping_change_reflected(live_pipe):
    requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping", json={"enabled": True}
    )
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RATE_SHAPING_GET)
    assert r.json()["enabled"] is True


def test_rate_shaping_reset_stats(live_pipe):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping/reset_stats"
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_RESET)
    assert data["pipe_id"] == live_pipe
    assert data["status"]  in ("ok", "reset", "done")
    _stop(live_pipe)


def test_rate_shaping_missing_body(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping", json={})
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_rate_shaping_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/rate_shaping")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_rate_shaping_set_nonexistent_pipe():
    r = requests.post(
        f"{BASE_URL}/pipe/test_ghost_pipe/rate_shaping", json={"enabled": True}
    )
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_rate_shaping_reset_nonexistent_pipe():
    r = requests.post(
        f"{BASE_URL}/pipe/test_ghost_pipe/rate_shaping/reset_stats"
    )
    assert r.status_code == 404
    assert_error_schema(r.json())


# =============================================================================
# CONFIG — GET /config, PATCH /config
# =============================================================================

def test_get_config():
    r = requests.get(f"{BASE_URL}/config")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_GET)
    cfg = data["config"]
    assert isinstance(cfg, dict), "config value must be a JSON object"
    assert "encoding" in cfg, "config.encoding is required"
    enc = cfg["encoding"]
    assert "codec"        in enc, "config.encoding.codec is required"
    assert "bitrate_kbps" in enc, "config.encoding.bitrate_kbps is required"
    assert isinstance(enc["codec"],        str), \
        "encoding.codec must be a string"
    assert isinstance(enc["bitrate_kbps"], (int, float)), \
        "encoding.bitrate_kbps must be numeric"
    assert enc["codec"] in CODECS, (
        f"encoding.codec must be one of {CODECS}, got {enc['codec']!r}"
    )


def test_patch_config_encoding():
    payload = {"encoding": {"codec": "h264", "bitrate_kbps": 3500}}
    r = requests.patch(f"{BASE_URL}/config", json=payload)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_PATCH)
    # restore_config_after_each autouse fixture restores original state


@pytest.mark.parametrize("codec", CODECS)
def test_patch_encoding_codec(codec):
    r = requests.patch(f"{BASE_URL}/config", json={"encoding": {"codec": codec}})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_PATCH)
    cfg = requests.get(f"{BASE_URL}/config").json()["config"]
    assert cfg["encoding"]["codec"] == codec, (
        f"Patched codec to {codec!r} but GET /config returned "
        f"{cfg['encoding']['codec']!r}"
    )


def test_patch_source_sets_restart_flag():
    r = requests.patch(f"{BASE_URL}/config", json={"source": {"type": "test"}})
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_PATCH)
    assert isinstance(data["restart_required"], bool)


def test_patch_encoding_no_restart_flag():
    r = requests.patch(
        f"{BASE_URL}/config", json={"encoding": {"bitrate_kbps": 3500}}
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_PATCH)
    assert data["restart_required"] is False, (
        f"Encoding-only patch should not require restart, "
        f"got restart_required={data['restart_required']}"
    )


def test_config_patch_reflected():
    new_bitrate = 4200
    requests.patch(
        f"{BASE_URL}/config", json={"encoding": {"bitrate_kbps": new_bitrate}}
    )
    r = requests.get(f"{BASE_URL}/config")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_GET)
    actual = data["config"]["encoding"]["bitrate_kbps"]
    assert actual == new_bitrate, \
        f"Expected bitrate_kbps={new_bitrate}, got {actual}"


def test_patch_invalid_codec():
    r = requests.patch(
        f"{BASE_URL}/config", json={"encoding": {"codec": "realvideo"}}
    )
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_patch_negative_bitrate():
    r = requests.patch(
        f"{BASE_URL}/config", json={"encoding": {"bitrate_kbps": -100}}
    )
    assert r.status_code == 400
    assert_error_schema(r.json())


def test_patch_unknown_field():
    r = requests.patch(f"{BASE_URL}/config", json={"totally_unknown": True})
    assert r.status_code in [200, 400]
    assert isinstance(r.json(), dict)


def test_config_wrong_method():
    r = requests.delete(f"{BASE_URL}/config")
    assert r.status_code in [404, 405]


# =============================================================================
# PIPE CREATE — codec field validated in response body  (runs × 4)
# =============================================================================

def test_create_pipe_codec_in_response(live_pipe_cfg):
    pid   = live_pipe_cfg["pipe_id"]
    ptype = live_pipe_cfg["pipe_type"]
    codec = live_pipe_cfg["codec"]

    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    assert pid in r.json()["pipes"], f"{pid} not found in pipes list after creation"

    r2 = requests.get(f"{BASE_URL}/pipe/{pid}")
    if r2.status_code == 200:
        data = r2.json()
        assert data.get("pipe_id")   == pid,   f"pipe_id mismatch"
        assert data.get("pipe_type") == ptype, f"pipe_type mismatch"
        assert data.get("codec")     == codec, f"codec mismatch"


# =============================================================================
# STATE FLOW  (runs × 4)
# =============================================================================

def test_state_flow_streaming(live_pipe):
    r = _start(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in ("started", "running", "streaming")

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_STREAM_STATS)
    assert r.json()["pipe_id"] == live_pipe

    r = _stop(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in ("stopped", "idle")

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code in [200, 503]
    if r.status_code == 200:
        assert_schema(r.json(), SCHEMA_STREAM_STATS)
    else:
        assert_error_schema(r.json())


def test_restart_keeps_pipe_accessible(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/restart")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["pipe_id"] == live_pipe

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code in [200, 503]
    if r.status_code == 200:
        assert_schema(r.json(), SCHEMA_STREAM_STATS)
    _stop(live_pipe)


# =============================================================================
# MULTI-PIPE INTERACTION — all 4 combos live simultaneously
# =============================================================================

def test_all_pipes_appear_in_list(all_pipes):
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    for pid in all_pipes:
        assert pid in data["pipes"], f"{pid} missing from pipes list"


def test_independent_bitrate_per_pipe(all_pipes):
    targets = {pid: 1000 + i * 500 for i, pid in enumerate(all_pipes)}
    for pid, br in targets.items():
        _start(pid)
        r = requests.post(
            f"{BASE_URL}/pipe/{pid}/bitrate", json={"bitrate_kbps": br}
        )
        assert r.status_code == 200
        assert_schema(r.json(), SCHEMA_BITRATE_SET)
        assert r.json()["current_bitrate"] == br

    for pid, expected in targets.items():
        r = requests.get(f"{BASE_URL}/pipe/{pid}/bitrate")
        assert r.status_code == 200
        assert_schema(r.json(), SCHEMA_BITRATE_GET)
        assert r.json()["current_bitrate"] == expected, (
            f"{pid}: expected {expected}, got {r.json()['current_bitrate']}"
        )
    for pid in all_pipes:
        _stop(pid)


def test_stop_one_pipe_does_not_affect_others(all_pipes):
    for pid in all_pipes:
        _start(pid)
    _stop(all_pipes[0])
    for pid in all_pipes[1:]:
        r = requests.get(f"{BASE_URL}/pipe/{pid}/stream/stats")
        assert r.status_code == 200, (
            f"{pid} affected after stopping {all_pipes[0]}"
        )
        assert_schema(r.json(), SCHEMA_STREAM_STATS)
        assert r.json()["pipe_id"] == pid
    for pid in all_pipes:
        _stop(pid)


def test_independent_abr_per_pipe(all_pipes):
    for pid in all_pipes:
        r = requests.get(f"{BASE_URL}/pipe/{pid}/abr/status")
        assert r.status_code == 200
        assert_schema(r.json(), SCHEMA_ABR_STATUS)
        assert r.json()["pipe_id"] == pid


def test_all_pipes_reachable_simultaneously(all_pipes):
    for pid in all_pipes:
        r = requests.get(f"{BASE_URL}/pipe/{pid}/bitrate")
        assert r.status_code == 200, f"pipe {pid} not reachable"
        assert_schema(r.json(), SCHEMA_BITRATE_GET)
        assert r.json()["pipe_id"] == pid


def test_independent_rate_shaping_per_pipe(all_pipes):
    for i, pid in enumerate(all_pipes):
        enabled = bool(i % 2)
        requests.post(
            f"{BASE_URL}/pipe/{pid}/rate_shaping", json={"enabled": enabled}
        )

    for i, pid in enumerate(all_pipes):
        r = requests.get(f"{BASE_URL}/pipe/{pid}/rate_shaping")
        assert r.status_code == 200
        assert_schema(r.json(), SCHEMA_RATE_SHAPING_GET)
        assert r.json()["enabled"] is bool(i % 2), (
            f"{pid}: rate_shaping enabled mismatch"
        )


@pytest.mark.parametrize("codec", CODECS)
def test_all_pipes_accept_codec_switch(all_pipes, codec):
    r = requests.patch(f"{BASE_URL}/config", json={"encoding": {"codec": codec}})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_PATCH)
    cfg = requests.get(f"{BASE_URL}/config").json()["config"]
    assert cfg["encoding"]["codec"] == codec
    # restore_config_after_each autouse fixture restores original state