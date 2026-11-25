"""标准库 HTTP 入口：接收持续到达的请求/副本状态并提供查询。

不依赖任何第三方包。路由：

* ``POST /ingest``        单条事件或事件数组（与 materials JSONL 同行格式）
* ``GET  /health``
* ``GET  /requests``      请求状态汇总
* ``GET  /requests/<id>`` 请求追查报告
* ``GET  /trace/<id>``    同上（精简别名）
* ``GET  /occupancy?by=pool|tenant|worker``
* ``GET  /quota``
* ``POST /reconcile``     对账（并持久化修复）

通过 ``--state-dir`` 可在进程重启后恢复两本账。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from .engine import RehearsalEngine
from .persist import Store


class RuntimeState:
    def __init__(self, state_dir: Optional[str] = None, materials_dir: Optional[str] = None):
        self.state_dir = Path(state_dir) if state_dir else None
        self.lock = threading.RLock()
        if self.state_dir and Store(self.state_dir).exists():
            self.engine = RehearsalEngine.restore(self.state_dir)
        elif materials_dir:
            from .config import RehearsalConfig
            self.engine = RehearsalEngine(RehearsalConfig.from_materials(materials_dir))
        else:
            self.engine = RehearsalEngine()

    def persist(self) -> None:
        if self.state_dir:
            Store(self.state_dir).save(self.engine)


def make_handler(state: RuntimeState):
    class Handler(BaseHTTPRequestHandler):
        server_version = "RehearsalRuntime/1.0"

        def log_message(self, fmt, *args):  # 保持终端安静
            return

        def _send(self, code: int, payload) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body_json(self):
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def do_GET(self):
            with state.lock:
                return self._do_GET()

        def _do_GET(self):
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            qs = parse_qs(parsed.query)
            eng = state.engine
            try:
                if parsed.path == "/health":
                    return self._send(200, {"ok": True, "clock_ms": eng.clock,
                                           "live_charges": len(eng.capacity.charges)})
                if parsed.path == "/requests":
                    return self._send(200, eng._status_counts())
                if len(parts) == 2 and parts[0] in ("requests", "trace"):
                    report = eng.request_report(parts[1])
                    code = 200 if report.get("found") else 404
                    return self._send(code, report)
                if parsed.path == "/occupancy":
                    by = qs.get("by", ["pool"])[0]
                    return self._send(200, eng.occupancy(by))
                if parsed.path == "/quota":
                    return self._send(200, eng.tenant_quota())
                return self._send(404, {"error": "not found", "path": parsed.path})
            except Exception as exc:  # noqa: BLE001
                return self._send(500, {"error": str(exc)})

        def do_POST(self):
            with state.lock:
                return self._do_POST()

        def _do_POST(self):
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/ingest":
                    data = self._body_json()
                    events = data if isinstance(data, list) else [data]
                    results = []
                    for event in events:
                        before = len(state.engine.trace)
                        state.engine.ingest(event)
                        results.append(state.engine.trace[before:])
                    state.persist()
                    return self._send(200, {"accepted": len(events), "trace": results})
                if parsed.path == "/reconcile":
                    report = state.engine.reconcile()
                    state.persist()
                    return self._send(200, report)
                if parsed.path == "/egress/reconnect":
                    flushed = state.engine.egress.reconnect()
                    state.persist()
                    return self._send(200, {"flushed": flushed})
                raise FileNotFoundError(parsed.path)
            except FileNotFoundError:
                return self._send(404, {"error": "not found"})
            except Exception as exc:  # noqa: BLE001
                return self._send(400, {"error": str(exc)})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 8765,
          state_dir: Optional[str] = None, materials_dir: Optional[str] = None) -> None:
    state = RuntimeState(state_dir, materials_dir)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    print(f"rehearsal runtime listening on http://{host}:{port}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        state.persist()
        httpd.server_close()
