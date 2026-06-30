import asyncio
import json
import os
import platform
import socket
import time

import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


# ── Intel RAPL Power ────────────────────────────────
RAPL_BASE = "/sys/class/powercap"
_power_state = {"energy_uj": None, "ts": None, "watts": None}


def _read_energy():
    total, found = 0, False
    try:
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

    prev_e, prev_t = _power_state["energy_uj"], _power_state["ts"]
    _power_state.update(energy_uj=energy, ts=now)

    if prev_e is None or prev_t is None:
        return None
    dt = now - prev_t
    de = energy - prev_e
    if dt <= 0 or de < 0:
        return _power_state["watts"]

    watts = round((de / 1_000_000) / dt, 1)
    _power_state["watts"] = watts
    return watts


# ── Network Speed ───────────────────────────────────
_net_state = {"sent": None, "recv": None, "ts": None}


def read_network():
    c = psutil.net_io_counters()
    now = time.time()
    result = {
        "up_kbps": 0,
        "down_kbps": 0,
        "total_sent_gb": round(c.bytes_sent / 1024**3, 2),
        "total_recv_gb": round(c.bytes_recv / 1024**3, 2),
    }
    if _net_state["sent"] is not None and _net_state["ts"] is not None:
        dt = now - _net_state["ts"]
        if dt > 0:
            result["up_kbps"] = round(
                (c.bytes_sent - _net_state["sent"]) / 1024 / dt, 1
            )
            result["down_kbps"] = round(
                (c.bytes_recv - _net_state["recv"]) / 1024 / dt, 1
            )
    _net_state.update(sent=c.bytes_sent, recv=c.bytes_recv, ts=now)
    return result


# ── Routes ──────────────────────────────────────────
@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    # ── initial system info ──
    try:
        freq = psutil.cpu_freq()
        freq_max = round(freq.max) if freq and freq.max else None
    except Exception:
        freq_max = None

    await ws.send_text(
        json.dumps(
            {
                "type": "info",
                "hostname": socket.gethostname(),
                "platform": f"{platform.system()} {platform.release()}",
                "cpu_count": psutil.cpu_count(logical=True),
                "cpu_physical": psutil.cpu_count(logical=False)
                or psutil.cpu_count(logical=True),
                "cpu_freq_mhz": freq_max,
                "ram_total_gb": round(
                    psutil.virtual_memory().total / 1024**3, 1
                ),
                "disk_total_gb": round(
                    psutil.disk_usage("/").total / 1024**3, 0
                ),
                "boot_time": psutil.boot_time(),
            }
        )
    )

    # ── periodic metrics ──
    try:
        while True:
            cpu = psutil.cpu_percent(interval=None)
            cpu_per_core = psutil.cpu_percent(interval=None, percpu=True)
            mem = psutil.virtual_memory()

            temp = None
            temps = psutil.sensors_temperatures()
            if temps:
                for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
                    if key in temps and temps[key]:
                        temp = round(
                            max(s.current for s in temps[key]), 1
                        )
                        break

            disk = psutil.disk_usage("/")

            await ws.send_text(
                json.dumps(
                    {
                        "type": "metrics",
                        "cpu": cpu,
                        "cpu_per_core": cpu_per_core,
                        "ram_pct": mem.percent,
                        "ram_used_gb": round(mem.used / 1024**3, 1),
                        "temp": temp,
                        "power": read_power(),
                        "disk_pct": round(disk.percent, 1),
                        "disk_used_gb": round(disk.used / 1024**3, 1),
                        "net": read_network(),
                        "uptime_s": round(
                            time.time() - psutil.boot_time()
                        ),
                    }
                )
            )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass


# Montar estáticos DESPUÉS de las rutas explícitas
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")