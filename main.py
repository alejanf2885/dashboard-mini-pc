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
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
LINKS_FILE = os.path.join(DATA_DIR, "links.json")

# ── Defaults ─────────────────────────────────────────
DEFAULT_SETTINGS: dict = {
    "telegram_token": "",
    "telegram_chat_id": "",
    "alerts": {
        "cpu":  {"threshold": 85, "duration_s": 60},
        "ram":  {"threshold": 90, "duration_s": 60},
        "temp": {"threshold": 80, "duration_s": 30},
        "disk": {"threshold": 90, "duration_s": 300},
    },
    "cooldown_minutes": 30,
    "ping_hosts": ["1.1.1.1", "8.8.8.8"],
    "adguard_url": "",
    "adguard_user": "",
    "adguard_password": "",
    "electricity_price": 0.15,  # €/kWh
}

# ── Alert state (per metric) ─────────────────────────
_alert_state: dict = {
    k: {"since": None, "last_alert": 0.0, "active": False}
    for k in ("cpu", "ram", "temp", "disk")
}

# ── Intel RAPL Power ─────────────────────────────────
RAPL_BASE = "/sys/class/powercap"
_power_state: dict = {"energy_uj": None, "ts": None, "watts": None}


def _read_energy() -> int | None:
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


def read_power() -> float | None:
    energy = _read_energy()
    now = time.time()
    if energy is None:
        return None
    prev_e, prev_t = _power_state["energy_uj"], _power_state["ts"]
    _power_state.update(energy_uj=energy, ts=now)
    if prev_e is None or prev_t is None:
        return None
    dt, de = now - prev_t, energy - prev_e
    if dt <= 0 or de < 0:
        return _power_state["watts"]
    watts = round((de / 1_000_000) / dt, 1)
    _power_state["watts"] = watts
    return watts


# ── Network total (via host /proc) ───────────────────
_net_state: dict = {"sent": None, "recv": None, "ts": None}


def _read_host_net():
    try:
        with open("/proc/1/net/dev") as f:
            lines = f.readlines()
        total_recv, total_sent = 0, 0
        for line in lines[2:]:
            parts = line.split()
            if len(parts) < 10:
                continue
            if parts[0].rstrip(":") == "lo":
                continue
            total_recv += int(parts[1])
            total_sent += int(parts[9])
        return total_sent, total_recv
    except Exception:
        return None, None


def read_network() -> dict:
    host_sent, host_recv = _read_host_net()
    if host_sent is not None:
        bytes_sent, bytes_recv = host_sent, host_recv
    else:
        c = psutil.net_io_counters()
        bytes_sent, bytes_recv = c.bytes_sent, c.bytes_recv
    now = time.time()
    result = {
        "up_kbps": 0, "down_kbps": 0,
        "total_sent_gb": round(bytes_sent / 1024**3, 2),
        "total_recv_gb": round(bytes_recv / 1024**3, 2),
    }
    if _net_state["sent"] is not None and _net_state["ts"] is not None:
        dt = now - _net_state["ts"]
        if dt > 0:
            result["up_kbps"]   = round((bytes_sent - _net_state["sent"]) / 1024 / dt, 1)
            result["down_kbps"] = round((bytes_recv - _net_state["recv"]) / 1024 / dt, 1)
    _net_state.update(sent=bytes_sent, recv=bytes_recv, ts=now)
    return result


# ── Per-interface network ────────────────────────────
_net_iface_state: dict = {}
_VIRT_PREFIXES = ("veth", "br-", "docker", "virbr", "lo", "dummy", "ifb")


def _is_physical(name: str) -> bool:
    return not any(name.startswith(p) for p in _VIRT_PREFIXES)


def _parse_proc_net_dev() -> dict:
    ifaces: dict = {}
    try:
        with open("/proc/1/net/dev") as f:
            for line in f.readlines()[2:]:
                parts = line.split()
                if len(parts) < 10:
                    continue
                ifaces[parts[0].rstrip(":")] = {
                    "recv": int(parts[1]), "sent": int(parts[9])
                }
    except Exception:
        pass
    return ifaces


def read_net_per_iface() -> list:
    now = time.time()
    raw = _parse_proc_net_dev()
    if not raw:
        try:
            raw = {
                k: {"sent": v.bytes_sent, "recv": v.bytes_recv}
                for k, v in psutil.net_io_counters(pernic=True).items()
            }
        except Exception:
            return []
    result = []
    for iface, c in raw.items():
        if not _is_physical(iface):
            continue
        prev = _net_iface_state.get(iface)
        up_kbps = down_kbps = 0.0
        if prev and prev["ts"]:
            dt = now - prev["ts"]
            if dt > 0:
                up_kbps   = max(round((c["sent"] - prev["sent"]) / 1024 / dt, 1), 0)
                down_kbps = max(round((c["recv"] - prev["recv"]) / 1024 / dt, 1), 0)
        _net_iface_state[iface] = {"sent": c["sent"], "recv": c["recv"], "ts": now}
        result.append({
            "iface": iface,
            "up_kbps": up_kbps, "down_kbps": down_kbps,
            "total_sent_gb": round(c["sent"] / 1024**3, 2),
            "total_recv_gb": round(c["recv"] / 1024**3, 2),
        })
    return result


# ── Disk I/O ─────────────────────────────────────────
_disk_io_state: dict = {"read": None, "write": None, "ts": None}


def read_disk_io() -> dict | None:
    try:
        c = psutil.disk_io_counters()
        if c is None:
            return None
        now = time.time()
        result = {"read_mbps": 0.0, "write_mbps": 0.0}
        if _disk_io_state["read"] is not None and _disk_io_state["ts"]:
            dt = now - _disk_io_state["ts"]
            if dt > 0:
                result["read_mbps"]  = round((c.read_bytes  - _disk_io_state["read"])  / 1024**2 / dt, 2)
                result["write_mbps"] = round((c.write_bytes - _disk_io_state["write"]) / 1024**2 / dt, 2)
        _disk_io_state.update(read=c.read_bytes, write=c.write_bytes, ts=now)
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


def read_disks() -> list:
    disks, seen = [], set()
    try:
        for part in psutil.disk_partitions():
            if part.fstype in _EXCLUDED_FS or part.device in seen:
                continue
            seen.add(part.device)
            try:
                u = psutil.disk_usage(part.mountpoint)
                disks.append({
                    "mount": part.mountpoint,
                    "device": part.device.replace("/dev/", ""),
                    "total_gb": round(u.total / 1024**3, 1),
                    "used_gb": round(u.used / 1024**3, 1),
                    "pct": round(u.percent, 1),
                })
            except (PermissionError, OSError):
                continue
    except Exception:
        pass
    return disks


# ── CPU Frequency per core ───────────────────────────
def read_cpu_freqs() -> list | None:
    freqs = []
    try:
        i = 0
        while True:
            path = f"/sys/devices/system/cpu/cpu{i}/cpufreq/scaling_cur_freq"
            if not os.path.exists(path):
                break
            with open(path) as f:
                freqs.append(round(int(f.read().strip()) / 1000))  # kHz → MHz
            i += 1
        if freqs:
            return freqs
    except Exception:
        pass
    # fallback: psutil
    try:
        pf = psutil.cpu_freq(percpu=True)
        if pf:
            return [round(f.current) for f in pf]
    except Exception:
        pass
    return None


# ── Per-core temperature ─────────────────────────────
def read_cpu_temps() -> list | None:
    try:
        sensors = psutil.sensors_temperatures()
        if not sensors:
            return None
        core_temps: dict = {}
        for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if key not in sensors:
                continue
            for entry in sensors[key]:
                label = entry.label.lower()
                if "core" in label:
                    digits = "".join(c for c in label if c.isdigit())
                    if digits:
                        core_temps[int(digits)] = round(entry.current, 1)
            if core_temps:
                mx = max(core_temps)
                return [core_temps.get(i) for i in range(mx + 1)]
    except Exception:
        pass
    return None


# ── Docker Containers ────────────────────────────────
_docker_cache: dict = {"data": [], "ts": 0.0}
DOCKER_CACHE_TTL = 10.0
# Matches Coolify-style hash suffixes: -<alphanum 8+> or -<digits 8+>
_COOLIFY_SUFFIX = re.compile(r"-[a-z0-9]{8,}$")
# Matches names that ARE entirely a hash (e.g. "vae4o3z5adlev11l2h4qvft5")
_ALL_HASH = re.compile(r"^[a-f0-9]{12,}$")


def _friendly_name(raw: str, labels: dict, image: str = "") -> str:
    # 1. Prefer explicit labels set by Coolify/compose
    for key in ("com.docker.compose.service", "coolify.name", "com.docker.compose.project.config_files"):
        val = labels.get(key, "")
        if val and not _ALL_HASH.match(val):
            return val
    # 2. Strip hash suffix
    cleaned = _COOLIFY_SUFFIX.sub("", raw)
    # 3. If what remains is still a pure hash, fall back to image name
    if _ALL_HASH.match(cleaned):
        return _short_image(image) or raw
    return cleaned or raw


def _short_image(image: str) -> str:
    return image.split(":")[0].split("/")[-1]


async def read_docker_containers() -> list:
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
                "name": _friendly_name(raw, labels, c.get("Image", "")),
                "image": _short_image(c["Image"]),
                "status": c["State"],
                "status_text": c["Status"],
            })
        _docker_cache.update(data=result, ts=now)
        return result
    except Exception:
        return _docker_cache["data"]


# ── Ping / latency ───────────────────────────────────
_ping_cache: dict = {"data": [], "ts": 0.0}
PING_CACHE_TTL = 15.0


async def _do_ping(host: str) -> dict:
    # ICMP ping via subprocess
    try:
        proc = await asyncio.create_subprocess_exec(
            "ping", "-c", "1", "-W", "2", "-q", host,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        m = re.search(r"rtt .* = ([\d.]+)/", stdout.decode())
        if m:
            return {"host": host, "ms": float(m.group(1)), "ok": True}
    except Exception:
        pass
    # Fallback: TCP to port 53
    try:
        start = time.monotonic()
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, 53), timeout=2.0
        )
        ms = round((time.monotonic() - start) * 1000, 1)
        writer.close()
        await writer.wait_closed()
        return {"host": host, "ms": ms, "ok": True}
    except Exception:
        return {"host": host, "ms": None, "ok": False}


async def read_ping(hosts: list) -> list:
    now = time.time()
    if now - _ping_cache["ts"] < PING_CACHE_TTL or not hosts:
        return _ping_cache["data"]
    results = await asyncio.gather(*[_do_ping(h) for h in hosts], return_exceptions=True)
    data = [r for r in results if isinstance(r, dict)]
    _ping_cache.update(data=data, ts=now)
    return data


# ── AdGuard Home ─────────────────────────────────────
_adguard_cache: dict = {"data": None, "ts": 0.0}
ADGUARD_CACHE_TTL = 60.0


async def read_adguard(settings: dict) -> dict | None:
    now = time.time()
    if now - _adguard_cache["ts"] < ADGUARD_CACHE_TTL:
        return _adguard_cache["data"]
    url = settings.get("adguard_url", "").rstrip("/")
    if not url:
        return None
    try:
        auth = None
        if settings.get("adguard_user"):
            auth = (settings["adguard_user"], settings.get("adguard_password", ""))
        async with httpx.AsyncClient(timeout=3.0, auth=auth, verify=False) as client:
            resp = await client.get(f"{url}/control/stats")
        if resp.status_code != 200:
            return None
        d = resp.json()
        total = max(d.get("num_dns_queries", 0), 1)
        blocked = d.get("num_blocked_filtering", 0)
        data = {
            "queries": d.get("num_dns_queries", 0),
            "blocked": blocked,
            "blocked_pct": round(blocked / total * 100, 1),
            "avg_ms": round(d.get("avg_processing_time", 0) * 1000, 2),
        }
        _adguard_cache.update(data=data, ts=now)
        return data
    except Exception:
        return _adguard_cache["data"]


# ── Service health checks ─────────────────────────────
_health_cache: dict = {}
HEALTH_CACHE_TTL = 30.0


async def check_service_health(links: list) -> dict:
    now = time.time()
    result = {}
    stale = [l for l in links if now - _health_cache.get(l["id"], {}).get("ts", 0) >= HEALTH_CACHE_TTL]
    # Use cached for non-stale
    for l in links:
        cached = _health_cache.get(l["id"])
        if cached and now - cached["ts"] < HEALTH_CACHE_TTL:
            result[l["id"]] = cached["status"]
    if not stale:
        return result
    try:
        async with httpx.AsyncClient(timeout=3.0, verify=False, follow_redirects=True) as client:
            async def _check(link):
                try:
                    resp = await client.get(link["url"])
                    return link["id"], "up" if resp.status_code < 500 else "down"
                except Exception:
                    return link["id"], "down"
            checks = await asyncio.gather(*[_check(l) for l in stale], return_exceptions=True)
        for item in checks:
            if isinstance(item, tuple):
                lid, status = item
                _health_cache[lid] = {"status": status, "ts": now}
                result[lid] = status
    except Exception:
        pass
    return result


# ── Settings ──────────────────────────────────────────
def _ensure_data():
    os.makedirs(DATA_DIR, exist_ok=True)


def load_settings() -> dict:
    _ensure_data()
    if not os.path.exists(SETTINGS_FILE):
        return DEFAULT_SETTINGS.copy()
    try:
        with open(SETTINGS_FILE) as f:
            saved = json.load(f)
        merged = {**DEFAULT_SETTINGS, **saved}
        merged["alerts"] = {**DEFAULT_SETTINGS["alerts"], **saved.get("alerts", {})}
        for k in DEFAULT_SETTINGS["alerts"]:
            merged["alerts"][k] = {**DEFAULT_SETTINGS["alerts"][k], **merged["alerts"].get(k, {})}
        return merged
    except Exception:
        return DEFAULT_SETTINGS.copy()


def save_settings(data: dict):
    _ensure_data()
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Telegram alerts ───────────────────────────────────
async def send_telegram(token: str, chat_id: str, text: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        return resp.status_code == 200
    except Exception:
        return False


async def check_alerts(metrics: dict):
    settings = load_settings()
    token = settings.get("telegram_token", "")
    chat_id = settings.get("telegram_chat_id", "")
    if not token or not chat_id:
        return

    cooldown_s = settings.get("cooldown_minutes", 30) * 60
    hostname = socket.gethostname()
    now = time.time()

    checks = [
        ("cpu",  metrics.get("cpu"),      "CPU",         "%"),
        ("ram",  metrics.get("ram_pct"),  "RAM",         "%"),
        ("temp", metrics.get("temp"),     "Temperatura", "°C"),
        ("disk", metrics.get("disk_pct"), "Disco",       "%"),
    ]

    for key, value, label, unit in checks:
        if value is None:
            continue
        cfg   = settings["alerts"].get(key, {})
        thresh   = cfg.get("threshold", 85)
        duration = cfg.get("duration_s", 60)
        state    = _alert_state[key]

        if value >= thresh:
            if state["since"] is None:
                state["since"] = now
            elapsed = now - state["since"]
            if elapsed >= duration and (now - state["last_alert"]) >= cooldown_s:
                mins = int(elapsed // 60)
                secs = int(elapsed % 60)
                text = (
                    f"⚠️ *{label} elevada* — `{hostname}`\n"
                    f"• Valor: `{value}{unit}`  _(umbral {thresh}{unit})_\n"
                    f"• Lleva: `{mins}m {secs}s` por encima\n"
                    f"_Próxima alerta en {settings.get('cooldown_minutes',30)} min si persiste_"
                )
                if await send_telegram(token, chat_id, text):
                    state["last_alert"] = now
                    state["active"] = True
        else:
            if state["active"] and state["since"] is not None:
                elapsed = now - state["since"]
                dur = int(elapsed // 60)
                text = (
                    f"✅ *{label} normalizada* — `{hostname}`\n"
                    f"• Valor actual: `{value}{unit}`\n"
                    f"• Duración del incidente: `{dur}m`"
                )
                await send_telegram(token, chat_id, text)
            state["since"] = None
            state["active"] = False


def get_active_alerts() -> list:
    now = time.time()
    result = []
    for key, state in _alert_state.items():
        if state["since"] is not None:
            result.append({
                "key": key,
                "since": state["since"],
                "elapsed_s": round(now - state["since"]),
                "active": state["active"],
            })
    return result


# ── SQLite ────────────────────────────────────────────
async def init_db():
    _ensure_data()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                ts REAL PRIMARY KEY,
                cpu REAL, ram_pct REAL, temp REAL, load1 REAL,
                net_down_kbps REAL, disk_pct REAL,
                disk_read_mbps REAL, disk_write_mbps REAL,
                power_w REAL
            )
        """)
        # Add column if upgrading from older schema
        try:
            await db.execute("ALTER TABLE metrics ADD COLUMN power_w REAL")
        except Exception:
            pass
        await db.commit()


async def store_metrics(payload: dict):
    try:
        io = payload.get("disk_io") or {}
        la = payload.get("load_avg") or [None]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?)",
                (payload.get("ts"), payload.get("cpu"), payload.get("ram_pct"),
                 payload.get("temp"), la[0],
                 (payload.get("net") or {}).get("down_kbps"),
                 payload.get("disk_pct"), io.get("read_mbps"), io.get("write_mbps"),
                 payload.get("power")),
            )
            await db.execute("DELETE FROM metrics WHERE ts < ?", (time.time() - 86400,))
            await db.commit()
    except Exception:
        pass


# ── Links ─────────────────────────────────────────────
def load_links() -> list:
    _ensure_data()
    if not os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, "w") as f:
            json.dump([], f)
    with open(LINKS_FILE) as f:
        return json.load(f)


def save_links(links: list):
    _ensure_data()
    with open(LINKS_FILE, "w") as f:
        json.dump(links, f, indent=2)


class LinkItem(BaseModel):
    name: str
    url: str
    icon: str = "🔗"
    color: str = "#3b82f6"
    description: str = ""


# ── Startup ───────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_db()


# ── REST Routes ───────────────────────────────────────
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
    links = [l for l in load_links() if l.get("id") != link_id]
    save_links(links)
    return {"ok": True}


@app.get("/api/settings")
async def get_settings():
    s = load_settings()
    # Never expose credentials in GET — mask them
    return {
        **s,
        "telegram_token": "***" if s.get("telegram_token") else "",
        "adguard_password": "***" if s.get("adguard_password") else "",
    }


@app.put("/api/settings")
async def put_settings(body: dict):
    current = load_settings()
    # Merge top-level fields
    for k, v in body.items():
        if k == "alerts" and isinstance(v, dict):
            for ak, av in v.items():
                if ak in current["alerts"] and isinstance(av, dict):
                    current["alerts"][ak].update(av)
        else:
            # Don't overwrite masked values
            if v not in ("***",):
                current[k] = v
    save_settings(current)
    return {"ok": True}


@app.post("/api/settings/test-telegram")
async def test_telegram():
    s = load_settings()
    if not s.get("telegram_token") or not s.get("telegram_chat_id"):
        return {"ok": False, "error": "Token o Chat ID no configurado"}
    ok = await send_telegram(
        s["telegram_token"], s["telegram_chat_id"],
        f"✅ *Test exitoso*\nDashboard `{socket.gethostname()}` conectado a Telegram"
    )
    return {"ok": ok}


@app.get("/api/history")
async def get_history(metric: str = "cpu", hours: float = 1.0):
    col_map = {
        "cpu": "cpu", "ram": "ram_pct", "temp": "temp", "load": "load1",
        "net_down": "net_down_kbps", "disk": "disk_pct",
        "disk_read": "disk_read_mbps", "disk_write": "disk_write_mbps",
        "power": "power_w",
    }
    col = col_map.get(metric, "cpu")
    since = time.time() - hours * 3600
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                f"SELECT ts, {col} FROM metrics WHERE ts > ? ORDER BY ts", (since,)
            ) as cur:
                rows = await cur.fetchall()
        return [{"ts": r[0], "v": r[1]} for r in rows]
    except Exception:
        return []


@app.get("/api/health")
async def get_health():
    return await check_service_health(load_links())


# ── WebSocket ─────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    interval = 2.0
    db_tick = 0
    alert_tick = 0

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

    await ws.send_text(json.dumps({
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
    }))

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

            try:
                disk = psutil.disk_usage("/")
                disk_pct = round(disk.percent, 1)
                disk_used_gb = round(disk.used / 1024**3, 1)
            except Exception:
                disk_pct = disk_used_gb = None

            try:
                la = os.getloadavg()
                load_avg = [round(x, 2) for x in la]
            except Exception:
                load_avg = None

            try:
                swap = psutil.swap_memory()
                swap_pct, swap_used_gb = swap.percent, round(swap.used / 1024**3, 1)
            except Exception:
                swap_pct = swap_used_gb = None

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

                top_procs_cpu = [_fmt(p) for p in
                    sorted(all_procs, key=lambda x: x.get("cpu_percent") or 0, reverse=True)[:10]]
                top_procs_ram = [_fmt(p) for p in
                    sorted(all_procs, key=lambda x: x.get("memory_percent") or 0, reverse=True)[:10]]
            except Exception:
                proc_count = None
                top_procs_cpu = top_procs_ram = []

            try:
                uptime_s = round(time.time() - psutil.boot_time())
            except Exception:
                uptime_s = None

            settings = load_settings()

            payload = {
                "type": "metrics",
                "ts": time.time(),
                "cpu": cpu,
                "cpu_per_core": cpu_per_core,
                "cpu_temps": read_cpu_temps(),
                "cpu_freqs": read_cpu_freqs(),
                "ram_pct": mem.percent,
                "ram_used_gb": round(mem.used / 1024**3, 1),
                "ram_cached_gb": round(getattr(mem, "cached", 0) / 1024**3, 1),
                "ram_buffers_gb": round(getattr(mem, "buffers", 0) / 1024**3, 1),
                "ram_available_gb": round(mem.available / 1024**3, 1),
                "temp": temp,
                "power": read_power(),
                "electricity_price": settings.get("electricity_price", 0.15),
                "disk_pct": disk_pct,
                "disk_used_gb": disk_used_gb,
                "disk_io": read_disk_io(),
                "disks": read_disks(),
                "net": read_network(),
                "net_ifaces": read_net_per_iface(),
                "load_avg": load_avg,
                "swap_pct": swap_pct,
                "swap_used_gb": swap_used_gb,
                "proc_count": proc_count,
                "top_procs_cpu": top_procs_cpu,
                "top_procs_ram": top_procs_ram,
                "uptime_s": uptime_s,
                "docker": await read_docker_containers(),
                "ping": await read_ping(settings.get("ping_hosts", [])),
                "adguard": await read_adguard(settings),
                "active_alerts": get_active_alerts(),
            }

            await ws.send_text(json.dumps(payload))

            # SQLite every ~10s
            db_tick += 1
            if db_tick >= max(1, round(10.0 / interval)):
                db_tick = 0
                asyncio.create_task(store_metrics(payload))

            # Alert check every ~20s
            alert_tick += 1
            if alert_tick >= max(1, round(20.0 / interval)):
                alert_tick = 0
                asyncio.create_task(check_alerts(payload))

            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        recv_task.cancel()


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
