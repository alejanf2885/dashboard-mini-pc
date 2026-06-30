import asyncio
import json
import time
import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

# ---------------- POWER ----------------
RAPL_BASE = "/sys/class/powercap"
_power_state = {"energy_uj": None, "ts": None, "watts": None}


def _read_energy():
    total = 0
    found = False

    try:
        import os
        for entry in os.listdir(RAPL_BASE):
            if entry.startswith("intel-rapl:"):
                path = f"{RAPL_BASE}/{entry}/energy_uj"
                if os.path.exists(path):
                    with open(path) as f:
                        total += int(f.read().strip())
                        found = True
    except Exception:
        return None

    return total if found else None


def read_power():
    energy = _read_energy()
    now = time.time()

    if energy is None:
        return None

    prev_e = _power_state["energy_uj"]
    prev_t = _power_state["ts"]

    _power_state["energy_uj"] = energy
    _power_state["ts"] = now

    if prev_e is None or prev_t is None:
        return None

    dt = now - prev_t
    de = energy - prev_e

    if dt <= 0 or de < 0:
        return _power_state["watts"]

    watts = round((de / 1_000_000) / dt, 1)
    _power_state["watts"] = watts
    return watts


# ---------------- WEBSOCKET ----------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    try:
        while True:
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()

            temp = None
            temps = psutil.sensors_temperatures()
            if temps:
                for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
                    if key in temps:
                        temp = max(t.current for t in temps[key])
                        break

            data = {
                "cpu": cpu,
                "ram_pct": mem.percent,
                "ram_used_gb": round(mem.used / 1024**3, 1),
                "temp": temp,
                "power": read_power(),
            }

            await ws.send_text(json.dumps(data))
            await asyncio.sleep(2)

    except WebSocketDisconnect:
        pass