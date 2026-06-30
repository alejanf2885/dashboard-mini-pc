import psutil
import time

def collect():
    cpu = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    temp = None
    temps = psutil.sensors_temperatures()
    if temps:
        for k in ("coretemp","k10temp","cpu_thermal"):
            if k in temps:
                temp = max(t.current for t in temps[k])
                break

    return {
        "ts": time.time(),
        "cpu": cpu,
        "ram": mem.percent,
        "disk": disk.percent,
        "temp": temp
    }