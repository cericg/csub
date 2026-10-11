"""``csub relay SOCKET``: a Unix socket for a sandbox on a host without LSF.

Brokers run only on LSF hosts (the submit host, compute nodes), so every policy and filesystem
check happens where the job will run. A sandbox elsewhere gets this socket instead of a broker's;
the relay runs outside the sandbox and passes each request to the broker over the client's
configured transport (ssh to the submit host). The ssh key stays outside the sandbox.

The relay forwards; it never interprets or rewrites a request. All validation is the broker's.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, TextIO

from csub.broker.serve import serve
from csub.protocol import CsubError, ProtocolError
from csub.transport import Transport


def forward(transport: Transport, raw: bytes) -> dict[str, Any]:
    """One request in, the broker's response out; failures to reach it become error responses."""
    try:
        try:
            request = json.loads(raw)
        except ValueError:
            raise ProtocolError("request: not valid JSON") from None
        if not isinstance(request, dict):
            raise ProtocolError("request: expected a JSON object")
        return transport.call(request)
    except CsubError as e:
        return e.to_response()


def relay(
    path: str,
    transport: Transport,
    *,
    stderr: TextIO,
    ready: Callable[[], None] | None = None,
) -> int:
    return serve(path, lambda raw: forward(transport, raw), stderr=stderr, ready=ready)
