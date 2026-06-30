import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, date

import aiosqlite
import httpx
import psutil
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

DB_PATH = os.getenv("DB_PATH", "/data/dashboard/metrics.db")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ALERT_CPU_THRESHOLD = int(os.getenv("ALERT_CPU_THRESHOLD", "85"))
ALERT_RAM_THRESHOLD = int(os.getenv("ALERT_RAM_THRESHOLD", "90"))
ALERT_TEMP_THRESHOLD = int(os.getenv("ALERT_TEMP_THRESHOLD", "75"))

_alert_cooldown: dict[str, float] = {}
ALERT_COOLDOWN_SECS = 600

RAPL_BASE = "/sys/class/powercap"
_power_state: dict[str, float] = {"energy_uj": None, "ts": None, "watts": None}


def _rapl_domains():
    domains = []
    try:
        for entry in os.listdir(RAPL_BASE):
            if entry.startswith("intel-rapl:") and entry.count(":") == 1:
                path = os.path.join(RAPL_BASE, entry, "energy_uj")
                max_path = os.path.join(RAPL_BASE, entry, "max_energy_range_uj")
                if os.path.exists(path):
                    domains.append((path, max_path))
    except Exception:
        pass
    return domains


def _read_energy_uj():
    total = 0
    found = False
    for path, _ in _rapl_domains():
        try:
            with open(path) as f:
                total += int(f.read().strip())
                found = True
        except Exception:
            pass
    return total if found else None


def read_power_watts():
    """Vatios del paquete CPU calculados sobre el delta de energía RAPL."""
    energy = _read_energy_uj()
    now = time.time()
    if energy is None:
        return None
    prev_e = _power_state["energy_uj"]
    prev_t = _power_state["ts"]
    _power_state["energy_uj"] = energy
    _power_state["ts"] = now
    if prev_e is None or prev_t is None:
        return _power_state["watts"]
    dt = now - prev_t
    de = energy - prev_e
    if de < 0:  # el contador RAPL se desbordó
        return _power_state["watts"]
    if dt <= 0:
        return _power_state["watts"]
    watts = round((de / 1_000_000) / dt, 1)
    _power_state["watts"] = watts
    return watts


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                cpu REAL,
                ram REAL,
                temp REAL,
                disk REAL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON metrics(ts)")
        try:
            await db.execute("ALTER TABLE metrics ADD COLUMN power REAL")
        except Exception:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                port TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                level TEXT,
                message TEXT
            )
        """)
        await db.commit()


async def collect_metrics():
    while True:
        try:
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            disk = psutil.disk_usage("/").percent
            temp = None
            temps = psutil.sensors_temperatures()
            if temps:
                for key in ("coretemp", "cpu_thermal", "k10temp", "acpitz"):
                    if key in temps and temps[key]:
                        temp = max(t.current for t in temps[key])
                        break

            power = read_power_watts()

            ts = int(time.time())
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO metrics (ts, cpu, ram, temp, disk, power) VALUES (?,?,?,?,?,?)",
                    (ts, cpu, ram, temp, disk, power),
                )
                await db.commit()

            await check_alerts(cpu, ram, temp)
        except Exception:
            pass
        await asyncio.sleep(60)


async def check_alerts(cpu: float, ram: float, temp: float | None):
    now = time.time()
    alerts = []
    if cpu > ALERT_CPU_THRESHOLD:
        alerts.append(("cpu", f"⚠️ CPU al {cpu:.0f}% (umbral: {ALERT_CPU_THRESHOLD}%)"))
    if ram > ALERT_RAM_THRESHOLD:
        alerts.append(("ram", f"⚠️ RAM al {ram:.0f}% (umbral: {ALERT_RAM_THRESHOLD}%)"))
    if temp and temp > ALERT_TEMP_THRESHOLD:
        alerts.append(("temp", f"🌡️ Temperatura {temp:.0f}°C (umbral: {ALERT_TEMP_THRESHOLD}°C)"))

    for key, msg in alerts:
        last = _alert_cooldown.get(key, 0)
        if now - last > ALERT_COOLDOWN_SECS:
            _alert_cooldown[key] = now
            await send_telegram(msg)
            await log_event("warning", msg)


async def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                timeout=10,
            )
    except Exception:
        pass


async def log_event(level: str, message: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO events (ts, level, message) VALUES (?,?,?)",
            (int(time.time()), level, message),
        )
        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(collect_metrics())
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return f.read()


@app.get("/api/current")
async def current():
    cpu = psutil.cpu_percent(interval=0.2)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    temp = None
    temps = psutil.sensors_temperatures()
    if temps:
        for key in ("coretemp", "cpu_thermal", "k10temp", "acpitz"):
            if key in temps and temps[key]:
                temp = round(max(t.current for t in temps[key]), 1)
                break
    return {
        "cpu": round(cpu, 1),
        "ram_used_gb": round(mem.used / 1024**3, 1),
        "ram_total_gb": round(mem.total / 1024**3, 1),
        "ram_pct": round(mem.percent, 1),
        "disk_used_gb": round(disk.used / 1024**3, 1),
        "disk_total_gb": round(disk.total / 1024**3, 1),
        "disk_pct": round(disk.percent, 1),
        "temp": temp,
        "power": read_power_watts(),
    }


@app.get("/api/history")
async def history(day: str | None = None):
    if day:
        d = datetime.strptime(day, "%Y-%m-%d").date()
    else:
        d = date.today()
    start = int(datetime(d.year, d.month, d.day).timestamp())
    end = start + 86400
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ts, cpu, ram, temp, power FROM metrics WHERE ts >= ? AND ts < ? ORDER BY ts",
            (start, end),
        ) as cur:
            rows = await cur.fetchall()
    return [{"ts": r[0], "cpu": r[1], "ram": r[2], "temp": r[3], "power": r[4]} for r in rows]


@app.get("/api/peaks")
async def peaks(day: str | None = None):
    if day:
        d = datetime.strptime(day, "%Y-%m-%d").date()
    else:
        d = date.today()
    start = int(datetime(d.year, d.month, d.day).timestamp())
    end = start + 86400
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ts, cpu, ram, temp, power FROM metrics WHERE ts >= ? AND ts < ?",
            (start, end),
        ) as cur:
            rows = await cur.fetchall()
    if not rows:
        return {}
    peak_cpu = max(rows, key=lambda r: r[1] or 0)
    peak_ram = max(rows, key=lambda r: r[2] or 0)
    peak_temp = max(rows, key=lambda r: r[3] or 0)
    peak_power = max(rows, key=lambda r: r[4] or 0)

    def fmt(ts):
        return datetime.fromtimestamp(ts).strftime("%H:%M")

    return {
        "cpu": {"value": round(peak_cpu[1], 1), "time": fmt(peak_cpu[0])},
        "ram": {"value": round(peak_ram[2], 1), "time": fmt(peak_ram[0])},
        "temp": {"value": round(peak_temp[3], 1) if peak_temp[3] else None, "time": fmt(peak_temp[0])},
        "power": {"value": round(peak_power[4], 1) if peak_power[4] else None, "time": fmt(peak_power[0])},
    }


@app.get("/api/discover")
async def discover():
    """Lista contenedores Docker con puertos publicados leyendo el socket."""
    def _list():
        import re
        import docker
        client = docker.from_env()
        results = []
        seen = set()
        for c in client.containers.list():
            name = c.labels.get("com.docker.compose.service") or re.sub(
                r"-[a-z0-9]{16,}$", "", c.name
            )
            ports = (c.attrs.get("NetworkSettings", {}).get("Ports") or {})
            for _cport, mappings in ports.items():
                if not mappings:
                    continue
                for m in mappings:
                    hp = m.get("HostPort")
                    if not hp or hp in seen:
                        continue
                    seen.add(hp)
                    results.append({"name": name, "port": hp})
        return results

    try:
        return await asyncio.to_thread(_list)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/services")
async def get_services():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, name, url, port FROM services ORDER BY id") as cur:
            rows = await cur.fetchall()
    return [{"id": r[0], "name": r[1], "url": r[2], "port": r[3]} for r in rows]


@app.post("/api/services")
async def add_service(request: Request):
    body = await request.json()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO services (name, url, port) VALUES (?,?,?)",
            (body["name"], body["url"], body.get("port", "")),
        )
        await db.commit()
    return {"ok": True}


@app.delete("/api/services/{service_id}")
async def delete_service(service_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM services WHERE id = ?", (service_id,))
        await db.commit()
    return {"ok": True}


@app.get("/api/events")
async def get_events():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ts, level, message FROM events ORDER BY ts DESC LIMIT 20"
        ) as cur:
            rows = await cur.fetchall()
    return [{"ts": r[0], "level": r[1], "message": r[2]} for r in rows]


@app.get("/api/stream")
async def stream():
    async def generator():
        while True:
            cpu = psutil.cpu_percent(interval=0.5)
            mem = psutil.virtual_memory()
            temp = None
            temps = psutil.sensors_temperatures()
            if temps:
                for key in ("coretemp", "cpu_thermal", "k10temp", "acpitz"):
                    if key in temps and temps[key]:
                        temp = round(max(t.current for t in temps[key]), 1)
                        break
            data = json.dumps({
                "cpu": round(cpu, 1),
                "ram_pct": round(mem.percent, 1),
                "ram_used_gb": round(mem.used / 1024**3, 1),
                "temp": temp,
                "power": read_power_watts(),
            })
            yield f"data: {data}\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(generator(), media_type="text/event-stream")


app.mount("/static", StaticFiles(directory="static"), name="static")
