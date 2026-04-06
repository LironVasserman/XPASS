import pytest
import requests
import time

BASE_URL = "http://192.168.1.21:8080"
TEST_PIPE_ID = "default"


# =====================================================
# HELPER
# =====================================================

def extract_bitrate(data: dict):
    if "current_bitrate" in data:
        return data["current_bitrate"]
    if "bitrate_kbps" in data:
        return data["bitrate_kbps"]
    if "bitrate" in data:
        return data["bitrate"]

    raise AssertionError(f"Bitrate field not found in response: {data}")


# =====================================================
# FIXTURE (Function Scope – isolated per test)
# =====================================================

@pytest.fixture(scope="function")
def pipe():

    payload = {
        "pipe_id": TEST_PIPE_ID,
        "pipe_type": "webrtc",
        "codec": "h265",
        "auto_start": False
    }

    requests.delete(f"{BASE_URL}/pipe/{TEST_PIPE_ID}")

    r = requests.post(f"{BASE_URL}/pipes", json=payload)
    assert r.status_code in [200]

    yield TEST_PIPE_ID

    requests.delete(f"{BASE_URL}/pipe/{TEST_PIPE_ID}")


# =====================================================
# HEALTH
# =====================================================

def test_health():
    assert requests.get(f"{BASE_URL}/health").status_code == 200


# =====================================================
# PIPE LIFECYCLE
# =====================================================

def test_start_pipe(pipe):
    r = requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    assert r.status_code in [200]


def test_stop_pipe(pipe):
    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    r = requests.post(f"{BASE_URL}/pipe/{pipe}/stop")
    assert r.status_code in [200]


def test_stop_then_start_flow(pipe):
    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    requests.post(f"{BASE_URL}/pipe/{pipe}/stop")
    r = requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    assert r.status_code in [200]


def test_restart_pipe(pipe):
    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    r = requests.post(f"{BASE_URL}/pipe/{pipe}/restart")
    assert r.status_code in [200]


# =====================================================
# STREAM STATS
# =====================================================

def test_stream_stats_running(pipe):

    requests.post(f"{BASE_URL}/pipe/{pipe}/start")

    r1 = requests.get(f"{BASE_URL}/pipe/{pipe}/stream/stats")
    if r1.status_code != 200:
        pytest.skip("Stats unavailable")

    stats1 = r1.json()
    time.sleep(1)

    r2 = requests.get(f"{BASE_URL}/pipe/{pipe}/stream/stats")
    assert r2.status_code == 200

    stats2 = r2.json()

    if "frames_sent" in stats1 and "frames_sent" in stats2:
        assert stats2["frames_sent"] >= stats1["frames_sent"]


# =====================================================
# BITRATE
# =====================================================

def test_bitrate_change_reflected(pipe):
    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    r_before = requests.get(f"{BASE_URL}/pipe/{pipe}/bitrate")
    if r_before.status_code != 200:
        pytest.skip("Bitrate endpoint unavailable")

    old_bitrate = extract_bitrate(r_before.json())
    new_bitrate = old_bitrate + 1000

    r = requests.post(
        f"{BASE_URL}/pipe/{pipe}/bitrate",
        json={"bitrate_kbps": new_bitrate}
    )
    assert r.status_code in [200]
    if r.status_code != 200:
        pytest.skip("Bitrate change rejected")

    time.sleep(10)

    r_after = requests.get(f"{BASE_URL}/pipe/{pipe}/bitrate")
    assert r_after.status_code == 200
    updated = extract_bitrate(r_after.json())
    assert updated == new_bitrate, f"Expected {new_bitrate}, got {updated}"

@pytest.mark.parametrize("bitrate", [0, 60000])
def test_invalid_bitrate_range(pipe, bitrate):

    requests.post(f"{BASE_URL}/pipe/{pipe}/start")

    r_before = requests.get(f"{BASE_URL}/pipe/{pipe}/bitrate")
    if r_before.status_code != 200:
        pytest.skip("Bitrate endpoint unavailable")

    old_bitrate = extract_bitrate(r_before.json())

    r = requests.post(
        f"{BASE_URL}/pipe/{pipe}/bitrate",
        json={"bitrate_kbps": bitrate}
    )

    assert r.status_code in [422]

    r_after = requests.get(f"{BASE_URL}/pipe/{pipe}/bitrate")

    if r_after.status_code == 200:
        new_bitrate = extract_bitrate(r_after.json())
        assert new_bitrate == old_bitrate


# =====================================================
# KEYFRAME
# =====================================================

def test_keyframe(pipe):
    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    r = requests.post(f"{BASE_URL}/pipe/{pipe}/keyframe")
    assert r.status_code in [200]


# =====================================================
# ABR
# =====================================================

def test_abr_endpoints(pipe):

    assert requests.get(
        f"{BASE_URL}/pipe/{pipe}/abr/status"
    ).status_code in [200]

    assert requests.get(
        f"{BASE_URL}/pipe/{pipe}/abr/mode"
    ).status_code in [200]

    assert requests.post(
        f"{BASE_URL}/pipe/{pipe}/abr/mode"
    ).status_code in [200]


# =====================================================
# CONFIG
# =====================================================

def test_get_config():
    assert requests.get(f"{BASE_URL}/config").status_code == 200


def test_patch_config_encoding():

    payload = {
        "encoding": {
            "codec": "h265",
            "bitrate_kbps": 3500
        }
    }

    r = requests.patch(f"{BASE_URL}/config", json=payload)
    assert r.status_code in [200]


def test_patch_encoding_shortcut():

    payload = {
        "codec": "h265",
        "bitrate_kbps": 4000
    }

    r = requests.patch(f"{BASE_URL}/config/encoding", json=payload)
    assert r.status_code in [200]


# =====================================================
# ERROR HANDLING
# =====================================================

def test_404_pipe_not_found():
    assert requests.get(
        f"{BASE_URL}/pipe/not_exists/bitrate"
    ).status_code == 404


@pytest.mark.parametrize("port", [1000, 70000])
def test_invalid_rtp_port_range(port):

    payload = {
        "pipe_id": f"bad_port_{port}",
        "rtp_port": port
    }

    r = requests.post(f"{BASE_URL}/pipes", json=payload)
    assert r.status_code in [400, 422]


def test_duplicate_pipe_conflict():

    payload = {"pipe_id": "dup_pipe_test"}

    requests.delete(f"{BASE_URL}/pipe/dup_pipe_test")

    r1 = requests.post(f"{BASE_URL}/pipes", json=payload)
    assert r1.status_code in [200]

    r2 = requests.post(f"{BASE_URL}/pipes", json=payload)
    assert r2.status_code == 409

    requests.delete(f"{BASE_URL}/pipe/dup_pipe_test")

    import time

    # =====================================================
    # FPS Runtime Change Verification (Not covered before)
    # =====================================================
    def test_runtime_fps_change(test_pipe):

        requests.post(f"{BASE_URL}/pipe/{test_pipe}/start")
        time.sleep(2)

        r_config = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_config.status_code == 200

        old_fps = r_config.json().get("fps")
        assert old_fps is not None

        new_fps = old_fps + 5 if old_fps < 55 else old_fps - 5

        r_patch = requests.patch(
            f"{BASE_URL}/pipe/{test_pipe}/config",
            json={"fps": new_fps}
        )

        assert r_patch.status_code in [200]

        time.sleep(2)

        r_verify = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_verify.status_code == 200
        assert r_verify.json().get("fps") == new_fps

        requests.post(f"{BASE_URL}/pipe/{test_pipe}/stop")

    # =====================================================
    # DSPC Toggle Verification (New)
    # =====================================================
    def test_dspc_toggle_runtime(test_pipe):

        r_config = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_config.status_code == 200

        old_value = r_config.json().get("dspc")

        # אם השדה לא קיים – אין טעם להכשיל ריצה
        if old_value is None:
            pytest.skip("DSPC field not present in config")

        new_value = not old_value

        r_patch = requests.patch(
            f"{BASE_URL}/pipe/{test_pipe}/config",
            json={"dspc": new_value}
        )

        assert r_patch.status_code in [200]

        time.sleep(1)

        r_verify = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_verify.status_code == 200
        assert r_verify.json().get("dspc") == new_value

    # =====================================================
    # Rate Shaping Toggle Verification (New)
    # =====================================================
    def test_rate_shaping_toggle_runtime(test_pipe):

        r_config = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_config.status_code == 200

        old_value = r_config.json().get("rate_shaping")

        if old_value is None:
            pytest.skip("rate_shaping field not present in config")

        new_value = not old_value

        r_patch = requests.patch(
            f"{BASE_URL}/pipe/{test_pipe}/config",
            json={"rate_shaping": new_value}
        )

        assert r_patch.status_code in [200]

        time.sleep(1)

        r_verify = requests.get(f"{BASE_URL}/pipe/{test_pipe}/config")
        assert r_verify.status_code == 200
        assert r_verify.json().get("rate_shaping") == new_value

# =====================================================
# ABR Mode Runtime Change Verification
# =====================================================

# =====================================================
# ABR Mode Runtime Change Verification (SPEC aligned)
# =====================================================

def test_runtime_abr_mode_change(pipe):

    requests.post(f"{BASE_URL}/pipe/{pipe}/start")
    time.sleep(1)

    # Get current mode
    r_before = requests.get(f"{BASE_URL}/pipe/{pipe}/abr/mode")
    assert r_before.status_code == 200

    old_mode = r_before.json().get("mode")
    assert old_mode is not None

    # According to SPEC:
    # POST /abr/mode switches pipe to "dynamic"
    r_switch = requests.post(f"{BASE_URL}/pipe/{pipe}/abr/mode")
    assert r_switch.status_code in [200]

    if r_switch.status_code != 200:
        pytest.skip("ABR switch not allowed (e.g. ethernet or not streaming)")

    time.sleep(1)

    # Verify mode is now dynamic
    r_after = requests.get(f"{BASE_URL}/pipe/{pipe}/abr/mode")
    assert r_after.status_code == 200
    assert r_after.json().get("mode") == "dynamic"


    # =====================================================
    # VALID PORT SHOULD SUCCEED
    # =====================================================

    def test_valid_rtp_port_success():

        test_port = 5004
        pipe_id = "valid_port_test"

        requests.delete(f"{BASE_URL}/pipe/{pipe_id}")

        payload = {
            "pipe_id": pipe_id,
            "pipe_type": "webrtc'",
            "codec": "h265",
            "rtp_port": test_port
        }

        r = requests.post(f"{BASE_URL}/pipes", json=payload)
        assert r.status_code in [200]

        requests.delete(f"{BASE_URL}/pipe/{pipe_id}")

    # =====================================================
    # PORT CONFLICT DETECTION
    # =====================================================

    def test_rtp_port_conflict():

        test_port = 5010

        payload1 = {
            "pipe_id": "port_conflict_1",
            "rtp_port": test_port
        }

        payload2 = {
            "pipe_id": "port_conflict_2",
            "rtp_port": test_port
        }

        requests.delete(f"{BASE_URL}/pipe/port_conflict_1")
        requests.delete(f"{BASE_URL}/pipe/port_conflict_2")

        r1 = requests.post(f"{BASE_URL}/pipes", json=payload1)
        assert r1.status_code in [200]

        r2 = requests.post(f"{BASE_URL}/pipes", json=payload2)
        assert r2.status_code in [400, 409, 422]

        requests.delete(f"{BASE_URL}/pipe/port_conflict_1")
        requests.delete(f"{BASE_URL}/pipe/port_conflict_2")

    # =====================================================
    # PORT SAVED IN CONFIG
    # =====================================================

    def test_rtp_port_persisted_in_config():

        test_port = 5020
        pipe_id = "port_config_test"

        requests.delete(f"{BASE_URL}/pipe/{pipe_id}")

        payload = {
            "pipe_id": pipe_id,
            "rtp_port": test_port
        }

        r = requests.post(f"{BASE_URL}/pipes", json=payload)
        assert r.status_code in [200]

        r_config = requests.get(f"{BASE_URL}/pipe/{pipe_id}/config")
        assert r_config.status_code == 200
        assert r_config.json().get("rtp_port") == test_port

        requests.delete(f"{BASE_URL}/pipe/{pipe_id}")

    # =====================================================
    # ENDPOINT RESPONSE CONTENT VALIDATION
    # =====================================================

    def test_stream_stats_content(pipe):

        requests.post(f"{BASE_URL}/pipe/{pipe}/start")

        r = requests.get(f"{BASE_URL}/pipe/{pipe}/stream/stats")
        assert r.status_code == 200

        data = r.json()

        assert isinstance(data, dict)
        assert "frames_sent" in data
        assert isinstance(data["frames_sent"], int)

    # =====================================================
    # BASIC RESPONSE SCHEMA VALIDATION
    # =====================================================

    def test_config_schema_validation():

        r = requests.get(f"{BASE_URL}/config")
        assert r.status_code == 200

        data = r.json()

        assert isinstance(data, dict)
        assert "encoding" in data
        assert isinstance(data["encoding"], dict)
        assert "codec" in data["encoding"]
        assert "bitrate_kbps" in data["encoding"]

    # =====================================================
    # RESPONSE TIME CHECK
    # =====================================================

    def test_health_response_time():

        start = time.time()
        r = requests.get(f"{BASE_URL}/health")
        duration = time.time() - start

        assert r.status_code == 200
        assert duration < 1.0

    # =====================================================
    # ENDPOINT LOAD (LIGHT SANITY LOAD)
    # =====================================================

    def test_health_under_light_load():

        for _ in range(20):
            r = requests.get(f"{BASE_URL}/health")
            assert r.status_code == 200

    # =====================================================
    # INVALID PAYLOAD VALIDATION
    # =====================================================

    def test_invalid_payload_validation():

        payload = {
            "pipe_id": "bad_payload_test",
            "rtp_port": "not_a_number",
            "codec": 123
        }

        r = requests.post(f"{BASE_URL}/pipes", json=payload)
        assert r.status_code in [400, 422]