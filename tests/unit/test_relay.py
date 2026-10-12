import io
import json
import os
import shutil
import tempfile
import threading

import pytest

from csub.client.relay import forward, relay
from csub.transport import TransportError, UnixTransport
from tests.support.procs import wait_until


class StubTransport:
    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, []

    def call(self, request):
        self.calls.append(request)
        if self.error:
            raise self.error
        return self.reply


def test_forward_passes_request_and_response_unchanged():
    reply = {"protocol": 1, "ok": False, "error": {"code": "policy_violation", "message": "m"}}
    t = StubTransport(reply)
    request = {"protocol": 1, "op": "submit", "job": {"command": ["x"], "mounts": "anything"}}
    assert forward(t, json.dumps(request).encode()) is reply
    assert t.calls == [request]


def test_forward_unreachable_broker_is_transport_error():
    resp = forward(StubTransport(error=TransportError("ssh h failed: refused")), b'{"op":"probe"}')
    assert resp["ok"] is False and resp["error"]["code"] == "transport_error"
    assert "refused" in resp["error"]["message"]


@pytest.mark.parametrize("raw", [b"garbage", b"[1]", b""])
def test_forward_rejects_non_objects_without_calling(raw):
    t = StubTransport({"protocol": 1, "ok": True})
    assert forward(t, raw)["error"]["code"] == "invalid_request" and t.calls == []


@pytest.fixture
def short_dir():
    d = tempfile.mkdtemp(prefix="csub-")  # Unix socket paths are limited to ~100 bytes
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_relay_roundtrip(short_dir, monkeypatch):
    # serve() installs signal handlers, which only works in the main thread; run it in a thread
    # with that part stubbed out and stop it by closing over a flag.
    import csub.broker.serve as serve_mod

    handlers = {}
    monkeypatch.setattr(serve_mod.signal, "signal", lambda sig, h: handlers.setdefault(sig, h))
    path = os.path.join(short_dir, "sock")
    t = StubTransport({"protocol": 1, "ok": True, "echo": 1})
    th = threading.Thread(target=relay, args=(path, t), kwargs={"stderr": io.StringIO()})
    th.start()
    try:
        wait_until(lambda: os.path.exists(path) and handlers, what="relay socket")
        assert UnixTransport(path).call({"protocol": 1, "op": "probe"})["echo"] == 1
        assert t.calls == [{"protocol": 1, "op": "probe"}]
        assert os.stat(path).st_mode & 0o777 == 0o600
    finally:
        handlers[serve_mod.signal.SIGTERM](serve_mod.signal.SIGTERM, None)
        th.join(timeout=5)
    assert not os.path.exists(path)
