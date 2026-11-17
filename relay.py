"""Powder Lines relay server: rooms with 8 character codes.

A player creates a room and gets a code, the others join with that code. The server only passes the game's
packets on between the players of a room (over WebSocket, so it also works in school / company networks).

Pure Python standard library, no packages needed.  Run:  python relay.py   (port from $PORT, default 8080)

Protocol
  text frames (JSON), player -> server:
    {"t": "host", "v": version}                         create a room           -> {"t": "room", "code": c, "id": 1}
    {"t": "join", "code": c, "v": version, "id": n}     join a room             -> {"t": "joined", "id": n, "peers": [...]}
    {"t": "kick", "id": n}                              host removes a player
    {"t": "ping"}                                                               -> {"t": "pong"}
  server -> player: {"t": "peer", "id": n}  {"t": "left", "id": n}  {"t": "closed"}  {"t": "error", "msg": "..."}
  binary frames: player -> server  [int32 target][uint8 flags][data]   (target 0 = all, -n = all except n)
                 server -> player  [int32 sender][uint8 flags][data]
"""
import asyncio
import base64
import hashlib
import json
import os
import random
import struct

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # no 0/O and 1/I: easy to read out loud
MAX_PLAYERS = 8
MAX_FRAME = 1 << 20

rooms = {}   # code -> Room


class Room:
    def __init__(self, code, version):
        self.code = code
        self.version = version
        self.peers = {}   # id -> Conn


class Conn:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.id = 0
        self.room = None
        self.lock = asyncio.Lock()
        self.closed = False

    async def send_frame(self, opcode, payload):
        if self.closed:
            return
        n = len(payload)
        if n < 126:
            head = struct.pack("!BB", 0x80 | opcode, n)
        elif n < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 126, n)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 127, n)
        try:
            async with self.lock:
                self.writer.write(head + payload)
                await self.writer.drain()
        except (ConnectionError, OSError):
            self.closed = True

    async def send_json(self, obj):
        await self.send_frame(0x1, json.dumps(obj).encode())

    async def send_bin(self, data):
        await self.send_frame(0x2, data)


async def read_frame(reader):
    """one complete message: (opcode, payload); continuation frames are joined"""
    data = b""
    first_op = None
    while True:
        b1, b2 = await reader.readexactly(2)
        fin = b1 & 0x80
        op = b1 & 0x0F
        masked = b2 & 0x80
        n = b2 & 0x7F
        if n == 126:
            (n,) = struct.unpack("!H", await reader.readexactly(2))
        elif n == 127:
            (n,) = struct.unpack("!Q", await reader.readexactly(8))
        if n > MAX_FRAME:
            raise ValueError("frame too large")
        mask = await reader.readexactly(4) if masked else b""
        payload = await reader.readexactly(n)
        if masked:
            payload = bytes(c ^ mask[i % 4] for i, c in enumerate(payload))
        if op >= 0x8:            # control frames can come between fragments
            return op, payload
        if first_op is None:
            first_op = op
        data += payload
        if fin:
            return first_op, data


def new_code():
    while True:
        code = "".join(random.choice(ALPHABET) for _ in range(8))
        if code not in rooms:
            return code


def clean_code(c):
    return "".join(ch for ch in str(c).upper() if ch.isalnum())


async def leave(conn):
    room = conn.room
    if room is None:
        return
    conn.room = None
    room.peers.pop(conn.id, None)
    if conn.id == 1:
        # the host left: the room is over
        rooms.pop(room.code, None)
        for other in list(room.peers.values()):
            other.room = None
            await other.send_json({"t": "closed"})
            await other.send_frame(0x8, b"")
        room.peers.clear()
    else:
        for other in list(room.peers.values()):
            await other.send_json({"t": "left", "id": conn.id})
        if not room.peers:
            rooms.pop(room.code, None)


async def on_text(conn, msg):
    t = msg.get("t")
    if t == "ping":
        await conn.send_json({"t": "pong"})
    elif t == "host" and conn.room is None:
        room = Room(new_code(), str(msg.get("v", "")))
        rooms[room.code] = room
        conn.id = 1
        conn.room = room
        room.peers[1] = conn
        await conn.send_json({"t": "room", "code": room.code, "id": 1})
    elif t == "join" and conn.room is None:
        room = rooms.get(clean_code(msg.get("code", "")))
        if room is None:
            await conn.send_json({"t": "error", "msg": "Diesen Raum gibt es nicht (mehr). Stimmt der Code?"})
            return
        if str(msg.get("v", "")) != room.version:
            await conn.send_json({"t": "error", "msg": "Der Raum nutzt eine andere Spielversion – bitte beide dieselbe ZIP."})
            return
        if len(room.peers) >= MAX_PLAYERS:
            await conn.send_json({"t": "error", "msg": "Der Raum ist voll (max. %d Spieler)." % MAX_PLAYERS})
            return
        want = msg.get("id")
        pid = want if isinstance(want, int) and 2 <= want < 2**31 and want not in room.peers else 0
        while pid == 0 or pid in room.peers:
            pid = random.randint(2, 2**31 - 1)
        others = list(room.peers.keys())
        conn.id = pid
        conn.room = room
        room.peers[pid] = conn
        await conn.send_json({"t": "joined", "id": pid, "peers": others, "code": room.code})
        for oid in others:
            await room.peers[oid].send_json({"t": "peer", "id": pid})
    elif t == "kick" and conn.room is not None and conn.id == 1:
        victim = conn.room.peers.get(msg.get("id"))
        if victim is not None:
            await victim.send_json({"t": "error", "msg": "Der Host hat dich aus dem Raum entfernt."})
            await leave(victim)
            await victim.send_frame(0x8, b"")


async def on_binary(conn, data):
    room = conn.room
    if room is None or len(data) < 5:
        return
    target, flags = struct.unpack("<iB", data[:5])
    out = struct.pack("<iB", conn.id, flags) + data[5:]
    if target > 0:
        dest = room.peers.get(target)
        if dest is not None and dest is not conn:
            await dest.send_bin(out)
    else:
        for pid, dest in list(room.peers.items()):
            if dest is not conn and pid != -target:
                await dest.send_bin(out)


async def handle(reader, writer):
    conn = Conn(reader, writer)
    try:
        request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 20)
        lines = request.decode("latin-1").split("\r\n")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        key = headers.get("sec-websocket-key")
        if headers.get("upgrade", "").lower() != "websocket" or not key:
            # plain HTTP (health check, waking the server up)
            body = ("Powder Lines Server laeuft. Raeume: %d\n" % len(rooms)).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body) + body)
            await writer.drain()
            return
        accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
        await writer.drain()
        while True:
            op, payload = await read_frame(reader)
            if op == 0x8:
                break
            if op == 0x9:
                await conn.send_frame(0xA, payload)
            elif op == 0x1:
                try:
                    msg = json.loads(payload.decode())
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    await on_text(conn, msg)
            elif op == 0x2:
                await on_binary(conn, payload)
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, asyncio.LimitOverrunError, ConnectionError, OSError, ValueError):
        pass
    finally:
        await leave(conn)
        conn.closed = True
        try:
            writer.close()
        except Exception:
            pass


async def main():
    port = int(os.environ.get("PORT", "8080"))
    server = await asyncio.start_server(handle, "0.0.0.0", port)
    print("Powder Lines relay on port", port, flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
