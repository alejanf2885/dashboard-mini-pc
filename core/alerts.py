import time

STATE = {
    "cpu": None,
    "ram": None,
    "temp": None
}

THRESHOLDS = {
    "cpu": 85,
    "ram": 90,
    "temp": 75
}

def evaluate(m):
    now = time.time()
    alerts = []

    for k in ["cpu","ram","temp"]:
        v = m.get(k)
        if v is None:
            continue

        if v > THRESHOLDS[k]:
            STATE[k] = STATE[k] or now

            if now - STATE[k] > 60:
                alerts.append({
                    "type": k,
                    "level": "warning",
                    "msg": f"{k.upper()} sostenido alto: {v}"
                })
        else:
            STATE[k] = None

    return alerts