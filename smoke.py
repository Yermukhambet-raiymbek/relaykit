#!/usr/bin/env python3
"""Run both real binaries against local HTTP, then verify recovery and restart.

Uses temporary storage and random, ephemeral secrets. Requires free local ports
18080 and 9090, Python 3, Go and a C compiler. Sends no external HTTP requests.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:18080"
# Ignore environment proxies for a strictly local smoke test.
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    processes = []
    with tempfile.TemporaryDirectory(prefix="relaykit-smoke-") as directory:
        work = Path(directory)
        env = os.environ | {
            "RELAY_API_TOKEN": secrets.token_hex(32),
            "RELAY_SIGNING_SECRET": secrets.token_hex(32),
            "RELAY_DB": str(work / "queue.db"),
            "RELAY_ADDR": "127.0.0.1:18080",
            "RELAY_ALLOW_LOCAL_DEMO": "true",
        }
        for name in ("relaykit", "receiver"):
            subprocess.run(["go", "build", "-buildvcs=false", "-o", str(work / name), "./cmd/" + name], cwd=ROOT, check=True)

        def start(name):
            process = subprocess.Popen([str(work / name)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            processes.append(process)
            return process

        def request(path, data=None, key=None):
            headers = {"Authorization": "Bearer " + env["RELAY_API_TOKEN"]}
            if key:
                headers["Idempotency-Key"] = key
            raw = None
            if data is not None:
                headers["Content-Type"] = "application/json"
                raw = json.dumps(data).encode()
            req = urllib.request.Request(BASE + path, data=raw, headers=headers)
            try:
                response = HTTP.open(req, timeout=3)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return response.status, json.load(response)

        def ready():
            for _ in range(60):
                try:
                    if request("/healthz")[0] == 200:
                        return
                except (OSError, urllib.error.URLError):
                    pass
                time.sleep(0.1)
            raise AssertionError("RelayKit did not become ready; ports 18080/9090 must be free")

        try:
            receiver = start("receiver")
            relay = start("relaykit")
            ready()
            assert receiver.poll() is None and relay.poll() is None, "local port already in use"
            body = {"destination": "http://127.0.0.1:9090/hook", "payload": {"event": "order.created", "order_id": 1001}}
            code, result = request("/v1/deliveries", body, "smoke-order-1001")
            assert code == 202 and result["created"]
            delivery_id = result["delivery"]["id"]
            code, duplicate = request("/v1/deliveries", body, "smoke-order-1001")
            assert code == 200 and not duplicate["created"]
            assert duplicate["delivery"]["id"] == delivery_id
            for _ in range(100):
                _, result = request("/v1/deliveries/" + delivery_id)
                if result["delivery"]["status"] == "delivered":
                    break
                time.sleep(0.1)
            assert result["delivery"]["status"] == "delivered", result
            codes = [attempt["status_code"] for attempt in result["attempts"]]
            assert codes == [503, 503, 200], codes
            changed = body | {"payload": {"event": "changed"}}
            assert request("/v1/deliveries", changed, "smoke-order-1001")[0] == 409
            stop(relay)
            start("relaykit")
            ready()
            _, persisted = request("/v1/deliveries/" + delivery_id)
            assert persisted["delivery"]["status"] == "delivered"
            assert len(persisted["attempts"]) == 3
            print(json.dumps({"result": "PASS", "attempt_statuses": codes, "duplicate_protected": True,
                              "conflict_rejected": True, "history_survived_restart": True}, indent=2))
        finally:
            for process in reversed(processes):
                stop(process)


if __name__ == "__main__":
    main()
