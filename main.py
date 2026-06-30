import asyncio
import json
import os
import platform
import socket
import time
import uuid

import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


app = FastAPI()
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


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


# ── Links / Services ───────────────────────────────
LINKS_FILE = os.path.join(DATA_DIR, "links.json")


class LinkItem(BaseModel):
    name: str
    url: str
    icon: str = "🔗"
    color: str = "#3b82f6"
    description: str = ""


def _ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, "w") as f:
            json.dump([], f)


def load_links():
    _ensure_data()
    with open(LINKS_FILE) as f:
        return json.load(f)


def save_links(links):
    _ensure_data()
    with open(LINKS_FILE, "w") as f:
        json.dump(links, f, indent=2)


# ── Routes ──────────────────────────────────────────
@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/links")
async def get_links():
    return load_links()


@app.post("/api/links")
async def create_link(link: LinkItem):
    links = load_links()
    entry = link.model_dump()
    entry["id"] = uuid.uuid4().hex[:8]
    links.append(entry)
    save_links(links)
    return entry


@app.put("/api/links/{link_id}")
async def update_link(link_id: str, link: LinkItem):
    links = load_links()
    for i, l in enumerate(links):
        if l.get("id") == link_id:
            updated = link.model_dump()
            updated["id"] = link_id
            links[i] = updated
            save_links(links)
            return updated
    return {"error": "not found"}


@app.delete("/api/links/{link_id}")
async def delete_link(link_id: str):
    links = load_links()
    links = [l for l in links if l.get("id") != link_id]
    save_links(links)
    return {"ok": True}


# ── WebSocket ───────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    # ── initial system info ──
    try:
        freq = psutil.cpu_freq()
        freq_max = round(freq.max) if freq and freq.max else None
    except Exception:
        freq_max = None

    try:
        disk_total = round(psutil.disk_usage("/").total / 1024**3, 0)
    except Exception:
        disk_total = None

    try:
        swap_total = round(psutil.swap_memory().total / 1024**3, 1)
    except Exception:
        swap_total = None

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
                "disk_total_gb": disk_total,
                "swap_total_gb": swap_total,
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

            # Temperature
            temp = None
            try:
                temps = psutil.sensors_temperatures()
                if temps:
                    for key in (
                        "coretemp",
                        "k10temp",
                        "cpu_thermal",
                        "acpitz",
                    ):
                        if key in temps and temps[key]:
                            temp = round(
                                max(s.current for s in temps[key]), 1
                            )
                            break
            except Exception:
                temp = None

            # Disk
            try:
                disk = psutil.disk_usage("/")
                disk_pct = round(disk.percent, 1)
                disk_used_gb = round(disk.used / 1024**3, 1)
            except Exception:
                disk_pct = None
                disk_used_gb = None

            # Network
            try:
                net = read_network()
            except Exception:
                net = {
                    "up_kbps": 0,
                    "down_kbps": 0,
                    "total_sent_gb": 0,
                    "total_recv_gb": 0,
                }

            # Load average
            try:
                la = os.getloadavg()
                load_avg = [round(x, 2) for x in la]
            except Exception:
                load_avg = None

            # Swap
            try:
                swap = psutil.swap_memory()
                swap_pct = swap.percent
                swap_used_gb = round(swap.used / 1024**3, 1)
            except Exception:
                swap_pct = None
                swap_used_gb = None

            # Processes
            try:
                all_procs = []
                for p in psutil.process_iter(
                    ["name", "cpu_percent", "memory_percent"]
                ):
                    try:
                        all_procs.append(p.info)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                proc_count = len(all_procs)
                all_procs.sort(
                    key=lambda x: x.get("cpu_percent") or 0, reverse=True
                )
                top_procs = [
                    {
                        "name": (p["name"] or "?")[:18],
                        "cpu": round(p.get("cpu_percent") or 0, 1),
                        "ram": round(p.get("memory_percent") or 0, 1),
                    }
                    for p in all_procs[:5]
                ]
            except Exception:
                proc_count = None
                top_procs = []

            # Uptime
            try:
                uptime_s = round(time.time() - psutil.boot_time())
            except Exception:
                uptime_s = None

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
                        "disk_pct": disk_pct,
                        "disk_used_gb": disk_used_gb,
                        "net": net,
                        "load_avg": load_avg,
                        "swap_pct": swap_pct,
                        "swap_used_gb": swap_used_gb,
                        "proc_count": proc_count,
                        "top_procs": top_procs,
                        "uptime_s": uptime_s,
                    }
                )
            )
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass


# Montar estáticos DESPUÉS de las rutas explícitas
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")