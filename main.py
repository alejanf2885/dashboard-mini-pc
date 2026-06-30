import asyncio
import json
import os
import platform
import re
import socket
import time
import uuid

import aiosqlite
import httpx
import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


app = FastAPI()
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "metrics.db")


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


# ── Network Speed (total, via host /proc) ───────────
_net_state = {"sent": None, "recv": None, "ts": None}


def _read_host_net():
    try:
        with open("/proc/1/net/dev") as f:
            lines = f.readlines()
        total_recv, total_sent = 0, 0
        for line in lines[2:]:
            parts = line.split()
            if len(parts) < 10:
                continue
            iface = parts[0].rstrip(":")
            if iface == "lo":
                continue
            total_recv += int(parts[1])
            total_sent += int(parts[9])
        return total_sent, total_recv
    except Exception:
        return None, None


def read_network():
    host_sent, host_recv = _read_host_net()
    if host_sent is not None:
        bytes_sent, bytes_recv = host_sent, host_recv
    else:
        c = psutil.net_io_counters()
        bytes_sent, bytes_recv = c.bytes_sent, c.bytes_recv

    now = time.time()
    result = {
        "up_kbps": 0,
        "down_kbps": 0,
        "total_sent_gb": round(bytes_sent / 1024**3, 2),
        "total_recv_gb": round(bytes_recv / 1024**3, 2),
    }
    if _net_state["sent"] is not None and _net_state["ts"] is not None:
        dt = now - _net_state["ts"]
        if dt > 0:
            result["up_kbps"] = round((bytes_sent - _net_state["sent"]) / 1024 / dt, 1)
            result["down_kbps"] = round((bytes_recv - _net_state["recv"]) / 1024 / dt, 1)
    _net_state.update(sent=bytes_sent, recv=bytes_recv, ts=now)
    return result


# ── Per-interface network ────────────────────────────
_net_iface_state: dict = {}


def read_net_per_iface():
    try:
        counters = psutil.net_io_counters(pernic=True)
        now = time.time()
        result = []
        for iface, c in counters.items():
            if iface == "lo":
                continue
            prev = _net_iface_state.get(iface)
            up_kbps, down_kbps = 0.0, 0.0
            if prev and prev["ts"]:
                dt = now - prev["ts"]
                if dt > 0:
                    up_kbps = round((c.bytes_sent - prev["sent"]) / 1024 / dt, 1)
                    down_kbps = round((c.bytes_recv - prev["recv"]) / 1024 / dt, 1)
            _net_iface_state[iface] = {"sent": c.bytes_sent, "recv": c.bytes_recv, "ts": now}
            result.append({
                "iface": iface,
                "up_kbps": up_kbps,
                "down_kbps": down_kbps,
                "total_sent_gb": round(c.bytes_sent / 1024**3, 2),
                "total_recv_gb": round(c.bytes_recv / 1024**3, 2),
            })
        return result
    except Exception:
        return []


# ── Disk I/O ─────────────────────────────────────────
_disk_io_state = {"read": None, "write": None, "ts": None}


def read_disk_io():
    try:
        counters = psutil.disk_io_counters()
        if counters is None:
            return None
        now = time.time()
        read_bytes = counters.read_bytes
        write_bytes = counters.write_bytes
        result = {"read_mbps": 0.0, "write_mbps": 0.0}
        if _disk_io_state["read"] is not None and _disk_io_state["ts"] is not None:
            dt = now - _disk_io_state["ts"]
            if dt > 0:
                result["read_mbps"] = round(
                    (read_bytes - _disk_io_state["read"]) / 1024**2 / dt, 2
                )
                result["write_mbps"] = round(
                    (write_bytes - _disk_io_state["write"]) / 1024**2 / dt, 2
                )
        _disk_io_state.update(read=read_bytes, write=write_bytes, ts=now)
        return result
    except Exception:
        return None


# ── Multiple Disks ───────────────────────────────────
_EXCLUDED_FS = {
    "tmpfs", "devtmpfs", "devfs", "overlay", "squashfs",
    "proc", "sysfs", "cgroup", "cgroup2", "pstore",
    "debugfs", "tracefs", "securityfs", "hugetlbfs",
    "mqueue", "fusectl", "bpf", "nsfs", "ramfs", "",
}


def read_disks():
    disks = []
    seen_devices: set = set()
    try:
        for part in psutil.disk_partitions():
            if part.fstype in _EXCLUDED_FS:
                continue
            if part.device in seen_devices:
                continue
            seen_devices.add(part.device)
            try:
                usage = psutil.disk_usage(part.mountpoint)
                disks.append({
                    "mount": part.mountpoint,
                    "device": part.device.replace("/dev/", ""),
                    "total_gb": round(usage.total / 1024**3, 1),
                    "used_gb": round(usage.used / 1024**3, 1),
                    "pct": round(usage.percent, 1),
                })
            except (PermissionError, OSError):
                continue
    except Exception:
        pass
    return disks


# ── Docker Containers ────────────────────────────────
_docker_cache: dict = {"data": [], "ts": 0.0}
DOCKER_CACHE_TTL = 10.0

# Coolify appends a random alphanum suffix like "-f8imbhdjwffwitoeg2tkm0h3"
_COOLIFY_SUFFIX = re.compile(r"-[a-z0-9]{15,}$")


def _friendly_name(raw: str, labels: dict) -> str:
    # prefer compose service label set by Coolify/Docker Compose
    for key in ("com.docker.compose.service", "coolify.name"):
        if labels.get(key):
            return labels[key]
    return _COOLIFY_SUFFIX.sub("", raw) or raw


def _short_image(image: str) -> str:
    # "ghcr.io/home-assistant/home-assistant:latest" → "home-assistant"
    return image.split(":")[ 0].split("/")[-1]


async def read_docker_containers():
    now = time.time()
    if now - _docker_cache["ts"] < DOCKER_CACHE_TTL:
        return _docker_cache["data"]
    try:
        transport = httpx.AsyncHTTPTransport(uds="/var/run/docker.sock")
        async with httpx.AsyncClient(transport=transport, timeout=2.0) as client:
            resp = await client.get("http://docker/containers/json?all=1")
        if resp.status_code != 200:
            return _docker_cache["data"]
        result = []
        for c in resp.json():
            raw = c["Names"][0].lstrip("/") if c["Names"] else c["Id"][:12]
            labels = c.get("Labels") or {}
            result.append({
                "id": c["Id"][:12],
                "name": _friendly_name(raw, labels),
                "image": _short_image(c["Image"]),
                "status": c["State"],
                "status_text": c["Status"],
            })
        _docker_cache.update(data=result, ts=now)
        return result
    except Exception:
        return _docker_cache["data"]


# ── Per-core temperature ─────────────────────────────
def read_cpu_temps():
    try:
        temps = psutil.sensors_temperatures()
        if not temps:
            return None
        core_temps: dict = {}
        for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if key not in temps:
                continue
            for entry in temps[key]:
                label = entry.label.lower()
                if "core" in label:
                    digits = "".join(c for c in label if c.isdigit())
                    if digits:
                        core_temps[int(digits)] = round(entry.current, 1)
            if core_temps:
                max_idx = max(core_temps)
                return [core_temps.get(i) for i in range(max_idx + 1)]
        return None
    except Exception:
        return None


# ── SQLite Persistence ───────────────────────────────
def _ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)


async def init_db():
    _ensure_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                ts REAL PRIMARY KEY,
                cpu REAL,
                ram_pct REAL,
                temp REAL,
                load1 REAL,
                net_down_kbps REAL,
                disk_pct REAL,
                disk_read_mbps REAL,
                disk_write_mbps REAL
            )
        """)
        await db.commit()


async def store_metrics(payload: dict):
    try:
        disk_io = payload.get("disk_io") or {}
        load_avg = payload.get("load_avg") or [None]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO metrics VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    payload.get("ts"),
                    payload.get("cpu"),
                    payload.get("ram_pct"),
                    payload.get("temp"),
                    load_avg[0] if load_avg else None,
                    (payload.get("net") or {}).get("down_kbps"),
                    payload.get("disk_pct"),
                    disk_io.get("read_mbps"),
                    disk_io.get("write_mbps"),
                ),
            )
            await db.execute(
                "DELETE FROM metrics WHERE ts < ?", (time.time() - 86400,)
            )
            await db.commit()
    except Exception:
        pass


# ── Links / Services ─────────────────────────────────
LINKS_FILE = os.path.join(DATA_DIR, "links.json")


class LinkItem(BaseModel):
    name: str
    url: str
    icon: str = "🔗"
    color: str = "#3b82f6"
    description: str = ""


def load_links():
    _ensure_data()
    if not os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, "w") as f:
            json.dump([], f)
    with open(LINKS_FILE) as f:
        return json.load(f)


def save_links(links):
    _ensure_data()
    with open(LINKS_FILE, "w") as f:
        json.dump(links, f, indent=2)


# ── Startup ──────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_db()


# ── Routes ───────────────────────────────────────────
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


@app.get("/api/history")
async def get_history(metric: str = "cpu", hours: float = 1.0):
    col_map = {
        "cpu": "cpu",
        "ram": "ram_pct",
        "temp": "temp",
        "load": "load1",
        "net_down": "net_down_kbps",
        "disk": "disk_pct",
        "disk_read": "disk_read_mbps",
        "disk_write": "disk_write_mbps",
    }
    col = col_map.get(metric, "cpu")
    since = time.time() - hours * 3600
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                f"SELECT ts, {col} FROM metrics WHERE ts > ? ORDER BY ts",
                (since,),
            ) as cursor:
                rows = await cursor.fetchall()
        return [{"ts": r[0], "v": r[1]} for r in rows]
    except Exception:
        return []


# ── WebSocket ─────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    interval = 2.0
    db_tick = 0

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
        json.dumps({
            "type": "info",
            "hostname": socket.gethostname(),
            "platform": f"{platform.system()} {platform.release()}",
            "cpu_count": psutil.cpu_count(logical=True),
            "cpu_physical": psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True),
            "cpu_freq_mhz": freq_max,
            "ram_total_gb": round(psutil.virtual_memory().total / 1024**3, 1),
            "disk_total_gb": disk_total,
            "swap_total_gb": swap_total,
            "boot_time": psutil.boot_time(),
        })
    )

    # listen for client commands (interval changes) in background
    async def receive_loop():
        nonlocal interval
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    msg = json.loads(raw)
                    if msg.get("cmd") == "set_interval":
                        interval = max(0.5, min(float(msg.get("secs", 2)), 30.0))
                except Exception:
                    pass
        except Exception:
            pass

    recv_task = asyncio.create_task(receive_loop())

    try:
        while True:
            cpu = psutil.cpu_percent(interval=None)
            cpu_per_core = psutil.cpu_percent(interval=None, percpu=True)
            mem = psutil.virtual_memory()

            # Temperature (max across sensors)
            temp = None
            try:
                sensors = psutil.sensors_temperatures()
                if sensors:
                    for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
                        if key in sensors and sensors[key]:
                            temp = round(max(s.current for s in sensors[key]), 1)
                            break
            except Exception:
                pass

            # Disk (root)
            try:
                disk = psutil.disk_usage("/")
                disk_pct = round(disk.percent, 1)
                disk_used_gb = round(disk.used / 1024**3, 1)
            except Exception:
                disk_pct = None
                disk_used_gb = None

            # Network (total)
            try:
                net = read_network()
            except Exception:
                net = {"up_kbps": 0, "down_kbps": 0, "total_sent_gb": 0, "total_recv_gb": 0}

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
                for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
                    try:
                        all_procs.append(p.info)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                proc_count = len(all_procs)

                def _fmt(p):
                    return {
                        "pid": p.get("pid"),
                        "name": (p.get("name") or "?")[:20],
                        "cpu": round(p.get("cpu_percent") or 0, 1),
                        "ram": round(p.get("memory_percent") or 0, 1),
                    }

                top_procs_cpu = [
                    _fmt(p) for p in
                    sorted(all_procs, key=lambda x: x.get("cpu_percent") or 0, reverse=True)[:10]
                ]
                top_procs_ram = [
                    _fmt(p) for p in
                    sorted(all_procs, key=lambda x: x.get("memory_percent") or 0, reverse=True)[:10]
                ]
            except Exception:
                proc_count = None
                top_procs_cpu = []
                top_procs_ram = []

            # Uptime
            try:
                uptime_s = round(time.time() - psutil.boot_time())
            except Exception:
                uptime_s = None

            payload = {
                "type": "metrics",
                "ts": time.time(),
                "cpu": cpu,
                "cpu_per_core": cpu_per_core,
                "cpu_temps": read_cpu_temps(),
                "ram_pct": mem.percent,
                "ram_used_gb": round(mem.used / 1024**3, 1),
                "temp": temp,
                "power": read_power(),
                "disk_pct": disk_pct,
                "disk_used_gb": disk_used_gb,
                "disk_io": read_disk_io(),
                "disks": read_disks(),
                "net": net,
                "net_ifaces": read_net_per_iface(),
                "load_avg": load_avg,
                "swap_pct": swap_pct,
                "swap_used_gb": swap_used_gb,
                "proc_count": proc_count,
                "top_procs_cpu": top_procs_cpu,
                "top_procs_ram": top_procs_ram,
                "uptime_s": uptime_s,
                "docker": await read_docker_containers(),
            }

            await ws.send_text(json.dumps(payload))

            # persist to SQLite every ~10s
            db_tick += 1
            if db_tick >= max(1, round(10.0 / interval)):
                db_tick = 0
                asyncio.create_task(store_metrics(payload))

            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        recv_task.cancel()


# mount statics after explicit routes
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
