"""
TCP Chat & File Share - SERVER

Run:  python server.py [port]

Why TCP?  TCP gives us a reliable, ordered byte stream between two programs.
Chat messages and file bytes arrive complete and in the same order they were
sent, with no loss or corruption. That is exactly what a chat/file app needs.

Message format (our own tiny protocol on top of TCP):
    [4 bytes: header length][JSON header][optional body of header["size"] bytes]
TCP is a *stream* (it has no message boundaries), so we must tell the
receiver how many bytes to read. That is why we send lengths first.
"""

import json
import os
import socket
import struct
import sys
import threading

HOST = "0.0.0.0"                 # listen on all network interfaces
PORT = 5000
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB limit for files

clients = {}                      # username -> Client
clients_lock = threading.Lock()   # protects the dict (many threads use it)


class Client:
    def __init__(self, conn, addr):
        self.conn = conn          # the TCP connection (socket) to this client
        self.addr = addr
        self.name = None
        self.send_lock = threading.Lock()  # one thread sends at a time


# ---------------------------------------------------------------- helpers
def recv_exact(conn, n):
    """Read exactly n bytes. recv() may return fewer bytes than asked,
    so we loop until we have all of them."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(min(65536, n - len(buf)))  # TCP: receive data
        if not chunk:             # empty bytes = the other side closed
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)


def send_packet(client, header, body=b""):
    """Send one packet (header + optional body) over TCP."""
    if body:
        header["size"] = len(body)
    data = json.dumps(header).encode("utf-8")
    with client.send_lock:
        # TCP: sendall() keeps sending until every byte is handed to TCP
        client.conn.sendall(struct.pack(">I", len(data)) + data + body)


def recv_packet(conn):
    """Receive one packet. Returns (header_dict, body_bytes)."""
    header_len = struct.unpack(">I", recv_exact(conn, 4))[0]
    if header_len > 65536:
        raise ValueError("header too large")
    header = json.loads(recv_exact(conn, header_len).decode("utf-8"))
    size = header.get("size", 0)
    if size > MAX_FILE_SIZE:
        raise ValueError("file too large")
    body = recv_exact(conn, size) if size else b""
    return header, body


def broadcast(header, body=b"", exclude=None):
    """Send a packet to every connected client except `exclude`."""
    with clients_lock:
        targets = [c for c in clients.values() if c is not exclude]
    for c in targets:
        try:
            send_packet(c, dict(header), body)
        except OSError:
            pass  # that client is gone; its own thread will clean it up


def info(client, text):
    send_packet(client, {"type": "info", "text": text})


# ------------------------------------------------------- per-client thread
def handle_client(client):
    """Runs in its own thread: one thread per connected client."""
    try:
        # First packet must be "join" with the username
        header, _ = recv_packet(client.conn)
        name = str(header.get("name", "")).strip()
        if header.get("type") != "join" or not name or " " in name:
            send_packet(client, {"type": "error",
                                 "text": "Invalid username (no spaces, not empty)."})
            return
        with clients_lock:
            if name in clients:
                send_packet(client, {"type": "error",
                                     "text": "Username already taken."})
                return
            client.name = name
            clients[name] = client

        print(f"[+] {name} connected from {client.addr[0]}:{client.addr[1]}")
        info(client, f"Welcome, {name}! Commands: /send <file>, /users, /quit")
        broadcast({"type": "info", "text": f"{name} joined the chat."}, exclude=client)

        while True:
            header, body = recv_packet(client.conn)
            kind = header.get("type")

            if kind == "msg":
                broadcast({"type": "msg", "from": name,
                           "text": str(header.get("text", ""))}, exclude=client)

            elif kind == "users":
                with clients_lock:
                    names = ", ".join(clients.keys())
                info(client, f"Online users: {names}")

            elif kind == "file":
                # Never trust the filename from the network: keep only the base name
                filename = os.path.basename(str(header.get("name", "file")))
                with clients_lock:
                    others = [c for c in clients.values() if c is not client]
                if not others:
                    info(client, "No one else is online to receive the file.")
                    continue
                print(f"[file] {name} -> {len(others)} client(s): {filename} ({len(body)} bytes)")
                for c in others:
                    try:
                        send_packet(c, {"type": "file", "from": name,
                                        "name": filename}, body)
                    except OSError:
                        pass
                info(client, f"File '{filename}' sent to {len(others)} user(s).")

            elif kind == "quit":
                break

    except (ConnectionError, OSError, ValueError, json.JSONDecodeError):
        pass  # client disconnected or sent bad data
    finally:
        # Cleanup: remove the client and close its TCP connection
        if client.name:
            with clients_lock:
                if clients.get(client.name) is client:
                    del clients[client.name]
            print(f"[-] {client.name} disconnected")
            broadcast({"type": "info", "text": f"{client.name} left the chat."})
        try:
            client.conn.close()
        except OSError:
            pass


# -------------------------------------------------------------------- main
def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT

    # TCP: SOCK_STREAM = TCP (SOCK_DGRAM would be UDP)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, port))   # attach the socket to an IP and port
    server.listen()             # TCP: start waiting for incoming connections
    print(f"Server listening on port {port}. Press Ctrl+C to stop.")

    try:
        while True:
            # TCP: accept() completes the 3-way handshake with one client
            # and returns a NEW socket just for talking to that client.
            conn, addr = server.accept()
            client = Client(conn, addr)
            threading.Thread(target=handle_client, args=(client,),
                             daemon=True).start()
    except KeyboardInterrupt:
        print("\nServer stopped.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
