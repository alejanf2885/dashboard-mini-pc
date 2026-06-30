import asyncio
import json

clients = set()

async def register(ws):
    clients.add(ws)

async def unregister(ws):
    clients.remove(ws)

async def broadcast(msg):
    dead = []

    for c in clients:
        try:
            await c.send_text(json.dumps(msg))
        except:
            dead.append(c)

    for d in dead:
        clients.remove(d)