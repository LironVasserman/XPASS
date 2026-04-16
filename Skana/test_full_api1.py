import time
import pytest
import requests

BASE_URL = "http://192.168.1.60:8080"

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


def assert_schema(data: dict, schema: dict) -> None:
    assert isinstance(data, dict), \
        f"Expected JSON object, got {type(data).__name__}: {data!r}"

    required = {k for k, (_, presence) in schema.items() if presence == "required"}
    allowed  = set(schema.keys())

    missing = required - data.keys()
    assert not missing, \
        f"Missing required keys: {missing}  —  got keys: {set(data.keys())}"

    extra = data.keys() - allowed
    assert not extra, (
        f"Response contains unexpected extra keys: {extra}  "
        f"(known keys: {allowed}). "
        "Add them to the schema if they are intentional."
    )

    for key, value in data.items():
        if key not in schema:
            continue
        if value is None:
            continue
        expected_types, _ = schema[key]
        if not isinstance(expected_types, tuple):
            expected_types = (expected_types,)
        assert isinstance(value, expected_types), (
            f"Field '{key}': expected {expected_types}, "
            f"got {type(value).__name__} = {value!r}"
        )


def assert_error_schema(data: dict) -> None:
    assert isinstance(data, dict), \
        f"Error body must be a JSON object, got: {data!r}"
    has_error   = "error"   in data and isinstance(data["error"],   str)
    has_message = "message" in data and isinstance(data["message"], str)
    has_detail  = "detail"  in data and isinstance(data["detail"],  (str, list, dict))
    assert has_error or has_message or has_detail, (
        f"Error response missing 'error'/'message'/'detail' key — got: {data}"
    )


def _unwrap_pipe(response_data: dict) -> dict:
    if "pipe" in response_data and isinstance(response_data["pipe"], dict):
        flat = dict(response_data["pipe"])
        flat.setdefault("status", response_data.get("status", "created"))
        return flat
    return response_data


def _pipe_ids_from_list(pipes_list: list) -> list:
    ids = []
    for entry in pipes_list:
        if isinstance(entry, str):
            ids.append(entry)
        elif isinstance(entry, dict):
            ids.append(entry["pipe_id"])
    return ids


def _pipe_id_in_list(pipe_id: str, pipes_list: list) -> bool:
    return pipe_id in _pipe_ids_from_list(pipes_list)


def _get_bitrate(data: dict) -> float:
    return data.get("bitrate_kbps", data.get("current_bitrate"))


def _get_fps(data: dict) -> float:
    return data.get("fps", data.get("current_fps"))


_STARTED_STATUSES = ("ok", "started", "running", "streaming")
_STOPPED_STATUSES = ("ok", "stopped", "idle")


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
    "pipe_id":              (str,            "required"),
    "pipe_type":            (str,            "required"),
    "codec":                (str,            "required"),
    "status":               (str,            "required"),
    "hardware":             (bool,           "optional"),
    "rtp_host":             (str,            "optional"),
    "rtp_port":             ((int, str),     "optional"),
    "current_bitrate_kbps": ((int, float),   "optional"),
    "stuck":                (bool,           "optional"),
    "abr_mode":             (str,            "optional"),
    "current_rssi_dbm":     ((int, float),   "optional"),
    "source":               ((str, dict),    "optional"),
    "viewer_url":           (str,            "optional"),
    "streaming":            (bool,           "optional"),
}

SCHEMA_PIPE_ACTION = {
    "pipe_id": (str, "optional"),
    "status":  (str, "required"),
    "message": (str, "optional"),
}

SCHEMA_BITRATE_GET = {
    "pipe_id":          (str,          "optional"),
    "bitrate_kbps":     ((int, float), "required"),
    "current_bitrate":  ((int, float), "optional"),
    "min_bitrate":      ((int, float), "optional"),
    "max_bitrate":      ((int, float), "optional"),
    "codec":            (str,          "optional"),
    "hardware_encoder": (bool,         "optional"),
}

SCHEMA_BITRATE_SET = {
    "pipe_id":          (str,          "optional"),
    "bitrate_kbps":     ((int, float), "required"),
    "current_bitrate":  ((int, float), "optional"),
    "codec":            (str,          "optional"),
    "hardware_encoder": (bool,         "optional"),
}

SCHEMA_FPS_GET = {
    "pipe_id":               (str,          "optional"),
    "fps":                   ((int, float), "required"),
    "fps_out":               ((int, float), "optional"),
    "current_fps":           ((int, float), "optional"),
    "min_fps":               ((int, float), "optional"),
    "max_fps":               ((int, float), "optional"),
    "frame_skipping_enabled":(bool,         "optional"),
}

SCHEMA_FPS_SET = {
    "pipe_id":               (str,          "optional"),
    "fps":                   ((int, float), "required"),
    "fps_out":               ((int, float), "optional"),
    "current_fps":           ((int, float), "optional"),
    "frame_skipping_enabled":(bool,         "optional"),
}

SCHEMA_RESOLUTION_GET = {
    "pipe_id":     (str,  "optional"),
    "resolution":  (str,  "required"),
    "width":       (int,  "optional"),
    "height":      (int,  "optional"),
    "live_change": (bool, "optional"),
    "clamped":     (bool, "optional"),
}

SCHEMA_RESOLUTION_SET = {
    "pipe_id":     (str,  "optional"),
    "resolution":  (str,  "required"),
    "width":       (int,  "optional"),
    "height":      (int,  "optional"),
    "live_change": (bool, "optional"),
    "clamped":     (bool, "optional"),
}

SCHEMA_KEYFRAME = {
    "pipe_id": (str, "required"),
    "status":  (str, "required"),
    "message": (str, "optional"),
}

SCHEMA_STREAM_STATS = {
    "pipe_id":            (str,          "required"),
    "frames_sent":        (int,          "required"),
    "bytes_sent":         (int,          "required"),
    "video_fps":          ((int, float), "required"),
    "bitrate_kbps":       ((int, float), "optional"),
    "dropped":            (int,          "optional"),
    "duration_s":         ((int, float), "optional"),
    "video_running":      (bool,         "optional"),
    "keyframes_sent":     (int,          "optional"),
    "total_connections":  (int,          "optional"),
    "signaling_running":  (bool,         "optional"),
    "active_connections": (int,          "optional"),
    "pipe_type":          (str,          "optional"),
    "video_bitrate_kbps": ((int, float), "optional"),
    "packet_loss_pct":    ((int, float), "optional"),
    "uptime_sec":         ((int, float), "optional"),
    "rtt_ms":             ((int, float), "optional"),
    "rtp_packets_sent":   (int,          "optional"),
}

SCHEMA_FRAME_TRACKING = {
    "pipe_id":             (str,  "required"),
    "enabled":             (bool, "required"),
    "total_frames_tracked":(int,  "optional"),
}

SCHEMA_FRAME_LATENCY = {
    "pipe_id":               (str,          "required"),
    "avg_latency_ms":        ((int, float), "optional"),
    "min_latency_ms":        ((int, float), "optional"),
    "max_latency_ms":        ((int, float), "optional"),
    "samples":               (int,          "optional"),
    "avg_encode_latency_ms": ((int, float), "optional"),
    "min_encode_latency_ms": ((int, float), "optional"),
    "max_encode_latency_ms": ((int, float), "optional"),
    "recent_correlations":   ((list, dict), "optional"),
    "total_frames_tracked":  (int,          "optional"),
}

SCHEMA_ABR_STATUS = {
    "pipe_id":              (str,             "required"),
    "enabled":              (bool,            "optional"),
    "abr_enabled":          (bool,            "optional"),
    "mode":                 (str,             "optional"),
    "bitrate_table":        ((list, dict),    "optional"),
    "transmission_type":    (str,             "optional"),
    "adapter_running":      (bool,            "optional"),
    "current_bitrate_kbps": ((int, float),    "optional"),
    "current_rssi_dbm":     ((int, float),    "optional"),
}

SCHEMA_ABR_MODE = {
    "pipe_id": (str,  "required"),
    "mode":    (str,  "required"),
    "enabled": (bool, "optional"),
    "status":  (str,  "optional"),
}

SCHEMA_RATE_SHAPING_GET = {
    "pipe_id":            (str,          "optional"),
    "enabled":            (bool,         "required"),
    "target_kbps":        ((int, float), "optional"),
    "burst_kbps":         ((int, float), "optional"),
    "mode":               (str,          "optional"),
    "avg_delay_ms":       ((int, float), "optional"),
    "tokens_available":   ((int, float), "optional"),
    "packets_dropped":    (int,          "optional"),
    "bucket_size_ms":     ((int, float), "optional"),
    "queue_depth":        (int,          "optional"),
    "bytes_sent":         (int,          "optional"),
    "actual_bitrate_bps": ((int, float), "optional"),
    "packets_sent":       (int,          "optional"),
    "strict_cbr":         (bool,         "optional"),
    "target_bitrate_bps": ((int, float), "optional"),
}

SCHEMA_RATE_SHAPING_SET = {
    "pipe_id":      (str,  "optional"),
    "enabled":      (bool, "optional"),
    "status":       (str,  "optional"),
    "rate_shaping": (dict, "optional"),
}

SCHEMA_RATE_SHAPING_RESET = {
    "pipe_id": (str, "required"),
    "status":  (str, "required"),
    "message": (str, "optional"),
}

SCHEMA_CONFIG_GET = {
    "config":          (dict, "required"),
    "last_updated":    (str,  "optional"),
    "streaming_active":(bool, "optional"),
}

SCHEMA_CONFIG_PATCH = {
    "restart_required": (bool, "required"),
    "config":           (dict, "optional"),
    "status":           (str,  "optional"),
    "changes":          ((dict, list), "optional"),
}


def _delete(pipe_id: str) -> None:
    requests.delete(f"{BASE_URL}/pipe/{pipe_id}")


def _safe_stop_delete(pipe_id: str) -> None:
    """Stop and delete a pipe, ignoring errors — safe to call in teardown.

    Tries three strategies in order:
    1. Direct URL (works for normal IDs)
    2. URL-encoded ID (for IDs with spaces, slashes, etc.)
    3. Iterates the /pipes list and sends DELETE with the raw pipe_id
       as a JSON body — fallback for servers that support it and for
       IDs that break URL routing entirely.
    """
    import urllib.parse

    def _attempt(url: str, method: str = "delete") -> None:
        try:
            fn = getattr(requests, method)
            fn(url, timeout=5)
        except Exception:
            pass

    # Strategy 1 — direct URL
    _attempt(f"{BASE_URL}/pipe/{pipe_id}/stop", "post")
    _attempt(f"{BASE_URL}/pipe/{pipe_id}", "delete")

    # Strategy 2 — URL-encoded
    encoded = urllib.parse.quote(pipe_id, safe="")
    if encoded != pipe_id:
        _attempt(f"{BASE_URL}/pipe/{encoded}/stop", "post")
        _attempt(f"{BASE_URL}/pipe/{encoded}", "delete")

    # Strategy 3 — verify via pipes list and delete any remaining match
    try:
        r = requests.get(f"{BASE_URL}/pipes", timeout=5)
        if r.status_code != 200:
            return
        for entry in r.json().get("pipes", []):
            listed_id = entry if isinstance(entry, str) else entry.get("pipe_id", "")
            if listed_id == pipe_id:
                # The pipe is still there — try encoded form one more time
                safe_id = urllib.parse.quote(listed_id, safe="")
                _attempt(f"{BASE_URL}/pipe/{safe_id}/stop", "post")
                _attempt(f"{BASE_URL}/pipe/{safe_id}", "delete")
                # Also try sending pipe_id as a query param if server supports it
                try:
                    requests.delete(
                        f"{BASE_URL}/pipes",
                        params={"pipe_id": listed_id},
                        timeout=5,
                    )
                except Exception:
                    pass
                break
    except Exception:
        pass


def _snapshot_pipe_ids() -> set:
    """Return the set of ALL pipe_ids currently registered on the server,
    including those with URL-unsafe characters."""
    r = requests.get(f"{BASE_URL}/pipes")
    if r.status_code != 200:
        return set()
    ids = set()
    for entry in r.json().get("pipes", []):
        if isinstance(entry, str):
            ids.add(entry)
        elif isinstance(entry, dict) and "pipe_id" in entry:
            ids.add(entry["pipe_id"])
    return ids


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


def _pipe_id_from_restart_response(data: dict, pipe_id: str) -> str:
    if "pipe_id" in data:
        return data["pipe_id"]
    status = data.get("status", "")
    if status in _STARTED_STATUSES + _STOPPED_STATUSES:
        return pipe_id
    raise AssertionError(
        f"Could not confirm pipe_id '{pipe_id}' from restart response: {data}. "
        f"'pipe_id' field is missing and status '{status}' is not a recognised success status."
    )


def _cam02_exists() -> bool:
    """Return True only if cam02 is present and reachable on the server."""
    r = requests.get(f"{BASE_URL}/pipes")
    if r.status_code != 200:
        return False
    return _pipe_id_in_list(_CAM02_ID, r.json().get("pipes", []))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def server_reachable():
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=5)
        r.raise_for_status()
    except Exception as exc:
        pytest.skip(
            f"Server at {BASE_URL} is not reachable — skipping all tests. "
            f"Reason: {exc}",
            allow_module_level=False,
        )


@pytest.fixture(scope="session")
def original_config(server_reachable):
    cfg = _get_config()
    yield cfg
    _restore_config(cfg)


@pytest.fixture(scope="session")
def pre_test_pipe_ids(server_reachable):
    return _snapshot_pipe_ids()


@pytest.fixture(scope="session", autouse=True)
def session_pipe_cleanup(pre_test_pipe_ids):
    yield
    # Delete every pipe that was not present before the test session started.
    # _safe_stop_delete handles URL-unsafe IDs (spaces, slashes, etc.) via
    # URL-encoding and a pipes-list fallback.
    pipes_after = _snapshot_pipe_ids()
    for pid in pipes_after - pre_test_pipe_ids:
        _safe_stop_delete(pid)


@pytest.fixture(autouse=True)
def restore_config_after_each(original_config):
    yield
    if original_config is not None:
        _restore_config(original_config)


@pytest.fixture(scope="function", params=PIPE_COMBOS, ids=COMBO_IDS)
def live_pipe(request):
    cfg = request.param
    _safe_stop_delete(cfg["pipe_id"])
    r = _create(cfg)
    assert r.status_code == 200, (
        f"Setup failed for {cfg['pipe_type']}/{cfg['codec']}: {r.text}"
    )
    yield cfg["pipe_id"]
    _safe_stop_delete(cfg["pipe_id"])


@pytest.fixture(scope="function", params=PIPE_COMBOS, ids=COMBO_IDS)
def live_pipe_cfg(request):
    cfg = request.param
    _safe_stop_delete(cfg["pipe_id"])
    r = _create(cfg)
    assert r.status_code == 200, (
        f"Setup failed for {cfg['pipe_type']}/{cfg['codec']}: {r.text}"
    )
    yield cfg
    _safe_stop_delete(cfg["pipe_id"])


@pytest.fixture(scope="function")
def all_pipes(server_reachable):
    ids = []
    for cfg in PIPE_COMBOS:
        _safe_stop_delete(cfg["pipe_id"])
        r = _create(cfg)
        assert r.status_code == 200, f"Setup failed for {cfg['pipe_id']}: {r.text}"
        pipe_data = _unwrap_pipe(r.json())
        ids.append(pipe_data.get("pipe_id", cfg["pipe_id"]))
    yield ids
    for pid in ids:
        _safe_stop_delete(pid)


@pytest.fixture(autouse=False)
def conflict_pipe_cleanup():
    _safe_stop_delete(_CONFLICT_PID)
    yield
    _safe_stop_delete(_CONFLICT_PID)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_health():
    r = requests.get(f"{BASE_URL}/health")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_HEALTH)


def test_health_status_ok():
    data = requests.get(f"{BASE_URL}/health").json()
    assert_schema(data, SCHEMA_HEALTH)
    assert data["status"] == "ok", f"Expected status='ok', got {data['status']!r}"
    assert data["uptime_seconds"] >= 0


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


def test_get_pipes():
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    assert data["total"] == len(data["pipes"]), (
        f"total={data['total']} but pipes list has {len(data['pipes'])} entries"
    )
    for entry in data["pipes"]:
        assert isinstance(entry, (str, dict)), \
            f"Each pipe entry must be a string or dict, got: {type(entry).__name__}"
        if isinstance(entry, dict):
            assert "pipe_id" in entry, f"Pipe object missing 'pipe_id': {entry!r}"


def test_pipes_list_updates():
    temp_id = "test_list_update_pipe"
    _safe_stop_delete(temp_id)
    try:
        r = _create({"pipe_id": temp_id})
        assert r.status_code == 200, f"Create failed: {r.text}"
        data = requests.get(f"{BASE_URL}/pipes").json()
        assert_schema(data, SCHEMA_PIPES_LIST)
        assert _pipe_id_in_list(temp_id, data["pipes"]), \
            f"{temp_id!r} not found in pipes list after creation"
    finally:
        _safe_stop_delete(temp_id)
        data = requests.get(f"{BASE_URL}/pipes").json()
        assert not _pipe_id_in_list(temp_id, data["pipes"]), \
            f"{temp_id!r} still present after deletion"
        assert data["total"] == len(data["pipes"])


@pytest.mark.parametrize("cfg", PIPE_COMBOS, ids=COMBO_IDS)
def test_create_and_delete(cfg):
    _safe_stop_delete(cfg["pipe_id"])
    try:
        r = _create(cfg)
        assert r.status_code == 200
        data = _unwrap_pipe(r.json())
        assert_schema(data, SCHEMA_PIPE_CREATE)
        assert data["pipe_id"]   == cfg["pipe_id"]
        assert data["pipe_type"] == cfg["pipe_type"]
        assert data["codec"]     == cfg["codec"]
        assert data["status"] in ("created", "stopped", "idle", "ok"), (
            f"Unexpected initial status: {data['status']!r}"
        )
    finally:
        _safe_stop_delete(cfg["pipe_id"])


def test_get_pipes_lists_all_created(all_pipes):
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    for pid in all_pipes:
        assert _pipe_id_in_list(pid, data["pipes"]), f"{pid} missing from pipes list"


def test_total_count_reflects_all_pipes(all_pipes):
    data = requests.get(f"{BASE_URL}/pipes").json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    assert data["total"] >= len(all_pipes)
    assert data["total"] == len(data["pipes"])


def test_duplicate_pipe_conflict():
    pid = "test_dup_pipe"
    _safe_stop_delete(pid)
    try:
        _create({"pipe_id": pid})
        r = _create({"pipe_id": pid})
        assert r.status_code == 409
        assert_error_schema(r.json())
    finally:
        _safe_stop_delete(pid)


def test_missing_pipe_id_returns_400():
    r = _create({})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


def test_invalid_pipe_type_returns_400():
    pid = "test_bad_type"
    _safe_stop_delete(pid)
    try:
        r = _create({"pipe_id": pid, "pipe_type": "unicorn"})
        assert r.status_code in (400, 422)
        assert_error_schema(r.json())
    finally:
        _safe_stop_delete(pid)


def test_invalid_codec_returns_400():
    pid = "test_bad_codec"
    _safe_stop_delete(pid)
    try:
        r = _create({"pipe_id": pid, "codec": "divx"})
        assert r.status_code in (400, 422)
        assert_error_schema(r.json())
    finally:
        _safe_stop_delete(pid)


def test_delete_nonexistent_pipe():
    r = requests.delete(f"{BASE_URL}/pipe/test_does_not_exist")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_start_stop_restart_pipe(live_pipe):
    r = _start(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in _STARTED_STATUSES, (
        f"start: unexpected status {r.json()['status']!r}"
    )

    r = _stop(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in _STOPPED_STATUSES, (
        f"stop: unexpected status {r.json()['status']!r}"
    )

    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/restart")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    restart_status = r.json()["status"]
    assert restart_status in _STARTED_STATUSES, (
        f"restart: expected a started status {_STARTED_STATUSES}, "
        f"got {restart_status!r}"
    )
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


def test_pipe_bitrate_get(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/bitrate")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_BITRATE_GET)
    assert _get_bitrate(r.json()) > 0, "bitrate must be positive"
    _stop(live_pipe)


def test_pipe_bitrate_set(live_pipe):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": 3000}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_BITRATE_SET)
    assert _get_bitrate(r.json()) > 0, "server must return a positive bitrate"
    _stop(live_pipe)


def test_bitrate_change_reflected(live_pipe):
    _start(live_pipe)
    new_bitrate = 3500
    set_r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": new_bitrate}
    )
    assert set_r.status_code == 200
    assert_schema(set_r.json(), SCHEMA_BITRATE_SET)
    set_value = _get_bitrate(set_r.json())
    assert set_value > 0, "SET response must return a positive bitrate"

    get_r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/bitrate")
    assert get_r.status_code == 200
    assert_schema(get_r.json(), SCHEMA_BITRATE_GET)
    get_value = _get_bitrate(get_r.json())
    assert get_value > 0, "GET response must return a positive bitrate"

    assert get_value == set_value, (
        f"GET bitrate ({get_value}) does not match the effective value "
        f"returned by SET ({set_value}). "
        f"Both should reflect the same post-clamp value."
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
    assert _get_bitrate(r.json()) > 0, "server must return a positive bitrate"
    _stop(live_pipe)


@pytest.mark.parametrize("bitrate", [-1, 0, 60000, 999999])
def test_invalid_bitrate_rejected(live_pipe, bitrate):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": bitrate}
    )
    assert r.status_code in (400, 422), (
        f"Expected 400/422 for bitrate={bitrate}, got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())
    _stop(live_pipe)


def test_bitrate_missing_field(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())
    _stop(live_pipe)


def test_bitrate_string_value(live_pipe):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate", json={"bitrate_kbps": "fast"}
    )
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())
    _stop(live_pipe)


def test_bitrate_malformed_json(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/bitrate",
        data="not-json",
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


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


def test_pipe_fps_get(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/fps")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_GET)
    assert _get_fps(r.json()) > 0, "fps must be positive"
    _stop(live_pipe)


def test_pipe_fps_set(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": 30})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_SET)
    assert _get_fps(r.json()) > 0, "server must return a positive fps"
    _stop(live_pipe)


def test_fps_change_reflected(live_pipe):
    _start(live_pipe)
    target = 25
    set_r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": target})
    assert set_r.status_code == 200
    set_value = _get_fps(set_r.json())
    assert set_value > 0

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/fps")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_GET)
    assert _get_fps(r.json()) == set_value, (
        f"GET fps does not match SET response: SET gave {set_value}, "
        f"GET gave {_get_fps(r.json())}"
    )
    _stop(live_pipe)


@pytest.mark.parametrize("fps", [30])
def test_valid_fps_boundaries(live_pipe, fps):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": fps})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_FPS_SET)
    assert _get_fps(r.json()) > 0, "server must return a positive fps value"
    _stop(live_pipe)


@pytest.mark.parametrize("fps", [0, -1, 999])
def test_invalid_fps_rejected(live_pipe, fps):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={"fps": fps})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())
    _stop(live_pipe)


def test_fps_missing_field(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/fps", json={})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())
    _stop(live_pipe)


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


def test_pipe_resolution_get(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/resolution")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RESOLUTION_GET)
    assert "x" in data["resolution"] or (
        "width" in data and "height" in data
    ), "resolution must be a WxH string or include width/height fields"
    _stop(live_pipe)


def test_pipe_resolution_set(live_pipe):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": 1280, "height": 720},
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RESOLUTION_SET)
    _stop(live_pipe)


def test_resolution_change_reflected(live_pipe):
    _start(live_pipe)
    target_w, target_h = 1920, 1080
    requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": target_w, "height": target_h},
    )
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/resolution")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RESOLUTION_GET)

    if "width" in data and "height" in data:
        assert data["width"]  == target_w, (
            f"width mismatch: expected {target_w}, got {data['width']}"
        )
        assert data["height"] == target_h, (
            f"height mismatch: expected {target_h}, got {data['height']}"
        )
    else:
        res = data["resolution"]
        parts = res.lower().replace(" ", "").split("x")
        assert len(parts) == 2, f"Cannot parse resolution string: {res!r}"
        got_w, got_h = int(parts[0]), int(parts[1])
        assert got_w == target_w, (
            f"width mismatch: expected {target_w}, got {got_w} (raw: {res!r})"
        )
        assert got_h == target_h, (
            f"height mismatch: expected {target_h}, got {got_h} (raw: {res!r})"
        )
    _stop(live_pipe)


@pytest.mark.parametrize("w,h", [(640, 480), (1920, 1080), (3840, 2160)])
def test_valid_resolutions(live_pipe, w, h):
    _start(live_pipe)
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": w, "height": h},
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RESOLUTION_SET)
    _stop(live_pipe)


@pytest.mark.parametrize("w,h", [(0, 720), (1280, 0), (-1, -1)])
def test_invalid_resolution_rejected(live_pipe, w, h):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/resolution",
        json={"width": w, "height": h},
    )
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


def test_resolution_missing_fields(live_pipe):
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/resolution", json={})
    assert r.status_code in (400, 422)
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


def test_stats_stopped_pipe(live_pipe_cfg):
    pipe_id   = live_pipe_cfg["pipe_id"]
    pipe_type = live_pipe_cfg["pipe_type"]

    _stop(pipe_id)
    time.sleep(1)

    r = requests.get(f"{BASE_URL}/pipe/{pipe_id}/stream/stats")

    if pipe_type == "rtp":
        assert r.status_code == 503, (
            f"rtp stopped pipe must return 503, got {r.status_code}: {r.text}"
        )
        assert_error_schema(r.json())
    else:
        assert r.status_code in (200, 503), (
            f"webrtc stopped pipe must return 200 or 503, got {r.status_code}: {r.text}"
        )
        if r.status_code == 200:
            data = r.json()
            assert_schema(data, SCHEMA_STREAM_STATS)
            assert data.get("video_running") is False, (
                f"webrtc stopped pipe returned 200 but video_running is not False: "
                f"{data.get('video_running')!r}"
            )
        else:
            assert_error_schema(r.json())


def test_stats_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/stream/stats")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_frame_tracking_get(live_pipe):
    _start(live_pipe)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/frames/tracking")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_FRAME_TRACKING)
    assert data["pipe_id"] == live_pipe
    assert isinstance(data["enabled"], bool)
    _stop(live_pipe)


def test_frame_tracking_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/frames/tracking")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_frame_latency_get():
    pid = "test_frame_latency_tracking"
    _safe_stop_delete(pid)
    r = _create({
        "pipe_id":        pid,
        "frame_tracking": True,
        "auto_start":     False,
    })
    assert r.status_code == 200, f"Failed to create pipe with frame_tracking: {r.text}"
    try:
        _start(pid)
        time.sleep(2)
        r = requests.get(f"{BASE_URL}/pipe/{pid}/frames/latency")
        assert r.status_code == 200, (
            f"Expected 200 after pipe running with frame_tracking=True, "
            f"got {r.status_code}: {r.text}"
        )
        data = r.json()
        assert_schema(data, SCHEMA_FRAME_LATENCY)
        assert data["pipe_id"] == pid
        latency = data.get("avg_latency_ms", data.get("avg_encode_latency_ms"))
        assert latency is not None, (
            f"Expected avg_latency_ms or avg_encode_latency_ms in response, got: {data}"
        )
        assert latency >= 0, f"latency must be non-negative, got {latency}"
    finally:
        _safe_stop_delete(pid)


def test_frame_latency_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/frames/latency")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_abr_status(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/abr/status")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_ABR_STATUS)
    assert data["pipe_id"] == live_pipe
    enabled = data.get("enabled", data.get("abr_enabled"))
    assert isinstance(enabled, bool)


def test_abr_mode_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/abr/mode")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_ABR_MODE)
    assert data["pipe_id"] == live_pipe
    assert len(data["mode"]) > 0, "mode must be a non-empty string"


def test_abr_mode_set(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode", json={"mode": "dynamic"}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_ABR_MODE)
    assert r.json()["pipe_id"] == live_pipe


def test_abr_mode_toggle(live_pipe):
    _DYNAMIC_MODES = {"dynamic"}
    _STATIC_MODES  = {"manual", "custom"}
    _ALL_MODES     = _DYNAMIC_MODES | _STATIC_MODES

    mode_before = requests.get(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode"
    ).json()["mode"]

    assert mode_before in _ALL_MODES, (
        f"Initial mode {mode_before!r} is not one of the known modes {_ALL_MODES}"
    )

    if mode_before in _DYNAMIC_MODES:
        target_mode    = "manual"
        expected_after = _STATIC_MODES
    else:
        target_mode    = "dynamic"
        expected_after = _DYNAMIC_MODES

    set_r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode", json={"mode": target_mode}
    )
    assert set_r.status_code == 200, (
        f"SET mode to {target_mode!r} failed: {set_r.status_code} {set_r.text}"
    )
    assert_schema(set_r.json(), SCHEMA_ABR_MODE)

    get_r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/abr/mode")
    assert get_r.status_code == 200
    assert_schema(get_r.json(), SCHEMA_ABR_MODE)
    mode_after = get_r.json()["mode"]

    assert mode_after in expected_after, (
        f"ABR mode was not switched: before={mode_before!r}, "
        f"requested={target_mode!r}, got={mode_after!r} "
        f"(expected one of {expected_after})"
    )


def test_abr_mode_invalid_rejected(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/abr/mode", json={"mode": "turbo"}
    )
    assert r.status_code in (400, 422), (
        f"Expected 400/422 for unknown mode, got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


def test_abr_status_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/abr/status")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_abr_mode_nonexistent_pipe():
    r = requests.get(f"{BASE_URL}/pipe/test_ghost_pipe/abr/mode")
    assert r.status_code == 404
    assert_error_schema(r.json())


def test_rate_shaping_get(live_pipe):
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_GET)
    assert isinstance(data["enabled"], bool)


def test_rate_shaping_set(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping", json={"enabled": True}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RATE_SHAPING_SET)


def test_rate_shaping_disable(live_pipe):
    r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping", json={"enabled": False}
    )
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_RATE_SHAPING_SET)


@pytest.mark.parametrize("enabled", [True, False])
def test_rate_shaping_change_reflected(live_pipe, enabled):
    set_r = requests.post(
        f"{BASE_URL}/pipe/{live_pipe}/rate_shaping", json={"enabled": enabled}
    )
    assert set_r.status_code == 200
    assert_schema(set_r.json(), SCHEMA_RATE_SHAPING_SET)

    get_r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping")
    assert get_r.status_code == 200
    assert_schema(get_r.json(), SCHEMA_RATE_SHAPING_GET)
    assert get_r.json()["enabled"] is enabled, (
        f"rate_shaping enabled={enabled} was not reflected: "
        f"GET returned {get_r.json()['enabled']!r}"
    )


def test_rate_shaping_reset_stats(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/rate_shaping/reset_stats")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_RATE_SHAPING_RESET)
    assert data["pipe_id"] == live_pipe
    assert data["status"]  in ("ok", "reset", "done")
    _stop(live_pipe)


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
    assert isinstance(enc["codec"],        str)
    assert isinstance(enc["bitrate_kbps"], (int, float))
    assert enc["codec"] in CODECS, (
        f"encoding.codec must be one of {CODECS}, got {enc['codec']!r}"
    )


def test_patch_config_encoding():
    r = requests.patch(f"{BASE_URL}/config",
                       json={"encoding": {"codec": "h264", "bitrate_kbps": 3500}})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_PATCH)


@pytest.mark.parametrize("codec", CODECS)
def test_patch_encoding_codec(codec):
    r = requests.patch(f"{BASE_URL}/config", json={"encoding": {"codec": codec}})
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_PATCH)
    cfg = requests.get(f"{BASE_URL}/config").json()["config"]
    assert cfg["encoding"]["codec"] == codec


def test_patch_source_sets_restart_flag():
    """
    Verify that patching the source section sets restart_required=True.
    We use restart_stream=False explicitly to tell the server NOT to apply
    the change immediately — only flag that a restart would be needed.
    This prevents the server from actually restarting the stream mid-test.
    """
    r = requests.patch(
        f"{BASE_URL}/config",
        json={"source": {"type": "test"}, "restart_stream": False},
    )
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_PATCH)
    assert data["restart_required"] is True, (
        f"Expected restart_required=True after source change, "
        f"got {data['restart_required']}"
    )


def test_patch_encoding_no_restart_flag():
    r = requests.patch(f"{BASE_URL}/config",
                       json={"encoding": {"bitrate_kbps": 3500}})
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_CONFIG_PATCH)
    assert data["restart_required"] is False, (
        f"Encoding-only patch should not require restart, "
        f"got restart_required={data['restart_required']}"
    )


def test_config_patch_reflected():
    new_bitrate = 4200
    requests.patch(f"{BASE_URL}/config",
                   json={"encoding": {"bitrate_kbps": new_bitrate}})
    r = requests.get(f"{BASE_URL}/config")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_CONFIG_GET)
    actual = r.json()["config"]["encoding"]["bitrate_kbps"]
    assert actual == new_bitrate, f"Expected {new_bitrate}, got {actual}"


def test_patch_invalid_codec():
    r = requests.patch(f"{BASE_URL}/config",
                       json={"encoding": {"codec": "realvideo"}})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


def test_patch_negative_bitrate():
    r = requests.patch(f"{BASE_URL}/config",
                       json={"encoding": {"bitrate_kbps": -100}})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


def test_patch_unknown_field():
    r = requests.patch(f"{BASE_URL}/config", json={"totally_unknown": True})
    assert r.status_code in (400, 422)
    assert_error_schema(r.json())


def test_config_wrong_method():
    r = requests.delete(f"{BASE_URL}/config")
    assert r.status_code in [404, 405]


def test_create_pipe_codec_in_response(live_pipe_cfg):
    pid   = live_pipe_cfg["pipe_id"]
    ptype = live_pipe_cfg["pipe_type"]
    codec = live_pipe_cfg["codec"]

    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    assert _pipe_id_in_list(pid, data["pipes"]), \
        f"{pid} not found in pipes list after creation"

    verify_id = f"{pid}_verify"
    _safe_stop_delete(verify_id)
    try:
        r2 = _create({
            "pipe_id":    verify_id,
            "pipe_type":  ptype,
            "codec":      codec,
            "auto_start": False,
        })
        assert r2.status_code == 200, f"Create verify pipe failed: {r2.text}"
        data2 = _unwrap_pipe(r2.json())
        assert_schema(data2, SCHEMA_PIPE_CREATE)
        assert data2.get("pipe_id")   == verify_id, \
            f"pipe_id mismatch: expected {verify_id!r}, got {data2.get('pipe_id')!r}"
        assert data2.get("pipe_type") == ptype, \
            f"pipe_type mismatch: expected {ptype!r}, got {data2.get('pipe_type')!r}"
        assert data2.get("codec")     == codec, \
            f"codec mismatch: expected {codec!r}, got {data2.get('codec')!r}"
    finally:
        _safe_stop_delete(verify_id)


def test_state_flow_streaming(live_pipe):
    r = _start(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in _STARTED_STATUSES

    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_STREAM_STATS)
    assert r.json()["pipe_id"] == live_pipe

    r = _stop(live_pipe)
    assert r.status_code == 200
    assert_schema(r.json(), SCHEMA_PIPE_ACTION)
    assert r.json()["status"] in _STOPPED_STATUSES

    time.sleep(1)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    if r.status_code == 200:
        data = r.json()
        assert_schema(data, SCHEMA_STREAM_STATS)
        assert data.get("video_running") is False, (
            f"Expected video_running=False after stop, got: {data.get('video_running')!r}"
        )
    else:
        assert r.status_code == 503, \
            f"Expected 503 or 200(video_running=False) after stop, got {r.status_code}: {r.text}"
        assert_error_schema(r.json())


def test_restart_keeps_pipe_accessible(live_pipe):
    _start(live_pipe)
    r = requests.post(f"{BASE_URL}/pipe/{live_pipe}/restart")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPE_ACTION)
    resolved_id = _pipe_id_from_restart_response(data, live_pipe)
    assert resolved_id == live_pipe, (
        f"pipe_id mismatch: expected '{live_pipe}', got '{resolved_id}'"
    )
    time.sleep(2)
    r = requests.get(f"{BASE_URL}/pipe/{live_pipe}/stream/stats")
    assert r.status_code == 200, \
        f"Expected 200 after restart, got {r.status_code}: {r.text}"
    assert_schema(r.json(), SCHEMA_STREAM_STATS)
    assert r.json()["pipe_id"] == live_pipe
    _stop(live_pipe)


def test_all_pipes_appear_in_list(all_pipes):
    r = requests.get(f"{BASE_URL}/pipes")
    assert r.status_code == 200
    data = r.json()
    assert_schema(data, SCHEMA_PIPES_LIST)
    for pid in all_pipes:
        assert _pipe_id_in_list(pid, data["pipes"]), \
            f"{pid} missing from pipes list"


def test_independent_bitrate_per_pipe(all_pipes):
    for pid in all_pipes:
        _start(pid)
        r = requests.post(
            f"{BASE_URL}/pipe/{pid}/bitrate", json={"bitrate_kbps": 3000}
        )
        assert r.status_code == 200
        assert_schema(r.json(), SCHEMA_BITRATE_SET)
        assert _get_bitrate(r.json()) > 0, f"{pid}: SET must return a positive bitrate"

    for pid in all_pipes:
        r1 = requests.get(f"{BASE_URL}/pipe/{pid}/bitrate")
        r2 = requests.get(f"{BASE_URL}/pipe/{pid}/bitrate")
        assert r1.status_code == 200
        assert_schema(r1.json(), SCHEMA_BITRATE_GET)
        v1 = _get_bitrate(r1.json())
        v2 = _get_bitrate(r2.json())
        assert v1 > 0, f"{pid}: GET must return a positive bitrate"
        assert v1 == v2, f"{pid}: bitrate is not stable between GET calls ({v1} vs {v2})"

    for pid in all_pipes:
        _stop(pid)


def test_stop_one_pipe_does_not_affect_others(all_pipes):
    for pid in all_pipes:
        _start(pid)
    _stop(all_pipes[0])
    time.sleep(1)  # allow the stop to propagate before querying other pipes
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
        _start(pid)
    for pid in all_pipes:
        r = requests.get(f"{BASE_URL}/pipe/{pid}/bitrate")
        assert r.status_code == 200, f"pipe {pid} not reachable"
        assert_schema(r.json(), SCHEMA_BITRATE_GET)
    for pid in all_pipes:
        _stop(pid)


def test_independent_rate_shaping_per_pipe(all_pipes):
    for i, pid in enumerate(all_pipes):
        requests.post(
            f"{BASE_URL}/pipe/{pid}/rate_shaping", json={"enabled": bool(i % 2)}
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


# ---------------------------------------------------------------------------
# cam02 conflict tests
# ---------------------------------------------------------------------------

_CAM02_ID     = "cam02"
_CAM02_TYPE   = "webrtc"
_CAM02_CODEC  = "h265"
_CAM02_VIEWER = "http://0.0.0.0:9002"
_CAM02_SOURCE = "rtsp://192.168.1.20:5005/routecam"
_CONFLICT_PID = "test_conflict_pipe"


@pytest.fixture(autouse=False)
def cam02_exists(server_reachable):
    """Skip the test if cam02 is not present on the server."""
    if not _cam02_exists():
        pytest.skip(f"pipe '{_CAM02_ID}' is not present on the server — skipping cam02 conflict tests")


def test_duplicate_pipe_id_of_cam02(conflict_pipe_cleanup, cam02_exists):
    r = _create({
        "pipe_id":   _CAM02_ID,
        "pipe_type": "rtp",
        "codec":     "h264",
    })
    assert r.status_code == 409, (
        f"Expected 409 when reusing pipe_id={_CAM02_ID!r}, "
        f"got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


def test_duplicate_viewer_url(conflict_pipe_cleanup, cam02_exists):
    r = _create({
        "pipe_id":    _CONFLICT_PID,
        "pipe_type":  _CAM02_TYPE,
        "codec":      _CAM02_CODEC,
        "viewer_url": _CAM02_VIEWER,
    })
    assert r.status_code in (409, 422), (
        f"Expected 409/422 for duplicate viewer_url={_CAM02_VIEWER!r}, "
        f"got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


def test_duplicate_source_url(conflict_pipe_cleanup, cam02_exists):
    r = _create({
        "pipe_id":   _CONFLICT_PID,
        "pipe_type": _CAM02_TYPE,
        "codec":     _CAM02_CODEC,
        "source":    _CAM02_SOURCE,
    })
    assert r.status_code in (409, 422), (
        f"Expected 409/422 for duplicate source={_CAM02_SOURCE!r}, "
        f"got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


def test_exact_cam02_payload_rejected(conflict_pipe_cleanup, cam02_exists):
    r = _create({
        "pipe_id":   _CAM02_ID,
        "pipe_type": "webrtc",
        "codec":     "h265",
    })
    assert r.status_code == 409, (
        f"Expected 409 when pipe_id={_CAM02_ID!r} is already taken, "
        f"got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


def test_cam02_id_with_different_fields_rejected(conflict_pipe_cleanup, cam02_exists):
    r = _create({
        "pipe_id":   _CAM02_ID,
        "pipe_type": "rtp",
        "codec":     "h264",
    })
    assert r.status_code == 409, (
        f"Expected 409 when pipe_id={_CAM02_ID!r} is already taken, "
        f"got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())


@pytest.mark.parametrize("bad_payload,description", [
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "rtp_port": -1},
        "negative rtp_port",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "rtp_port": 99999},
        "rtp_port above valid range",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "current_bitrate_kbps": -50},
        "negative bitrate_kbps",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "abr_mode": "ultramax"},
        "unknown abr_mode value",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "viewer_url": "not-a-url"},
        "malformed viewer_url",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "source": 12345},
        "source as integer",
    ),
    (
        {"pipe_id": _CONFLICT_PID, "pipe_type": "webrtc", "codec": "h265",
         "hardware": "yes"},
        "hardware as string instead of boolean",
    ),
])
def test_create_pipe_invalid_field_rejected(bad_payload, description, conflict_pipe_cleanup):
    r = _create(bad_payload)
    assert r.status_code in (400, 422), (
        f"[{description}] Expected 400/422, got {r.status_code}: {r.text}"
    )
    assert_error_schema(r.json())
    pipes = requests.get(f"{BASE_URL}/pipes").json().get("pipes", [])
    assert not _pipe_id_in_list(_CONFLICT_PID, pipes), (
        f"[{description}] Pipe {_CONFLICT_PID!r} was created despite invalid payload"
    )


def test_empty_string_pipe_id_rejected():
    pid = ""
    try:
        r = _create({"pipe_id": pid, "pipe_type": "webrtc", "codec": "h265"})
        assert r.status_code in (400, 422), (
            f"Expected 400/422 for empty pipe_id, got {r.status_code}: {r.text}"
        )
        assert_error_schema(r.json())
    finally:
        _safe_stop_delete(pid)


@pytest.mark.parametrize("bad_id", [
    "pipe with spaces",
    "pipe.dot",
    "pipe\nnewline",
    "..traversal",
])
def test_pipe_id_illegal_characters_rejected(bad_id):
    """
    These IDs contain characters that break URL routing (/  spaces  newlines).
    If the server mistakenly accepts one, it may be impossible to delete it
    via the REST API — so we assert rejection AND verify the pipe was NOT created.
    """
    r = _create({"pipe_id": bad_id, "pipe_type": "webrtc", "codec": "h265"})
    assert r.status_code in (400, 422), (
        f"Expected 400/422 for pipe_id={bad_id!r}, got {r.status_code}: {r.text}\n"
        f"WARNING: if the server accepted this ID it may be impossible to delete via REST API."
    )
    assert_error_schema(r.json())

    # Verify the pipe was NOT created — if it was, the server has a bug
    # and the pipe may persist permanently (cannot be cleaned up via URL routing).
    pipes = _snapshot_pipe_ids()
    assert bad_id not in pipes, (
        f"CRITICAL: pipe_id={bad_id!r} was created despite containing illegal characters. "
        f"This pipe cannot be deleted via the REST API and will persist on the server."
    )
