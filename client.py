"""
TCP Chat & File Share - CLIENT

Run:  python client.py [server_ip] [port]
      (defaults: 127.0.0.1 and 5000)

Two threads:
  - main thread      : reads what you type and SENDS it
  - receiver thread  : waits for data from the server and PRINTS it
This lets you send and receive at the same time.
"""

import json
import os
import socket
import struct
import sys
import threading

MAX_FILE_SIZE = 50 * 1024 * 1024  # must match the server limit
DOWNLOAD_DIR = "downloads"


# ---------------------------------------------------------------- helpers
def recv_exact(sock, n):
    """Read exactly n bytes (TCP recv() can return less than requested)."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(65536, n - len(buf)))  # TCP: receive data
        if not chunk:
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)


def send_packet(sock, header, body=b""):
    """Send [4-byte header length][JSON header][body] over TCP."""
    if body:
        header["size"] = len(body)
    data = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack(">I", len(data)) + data + body)  # TCP: send data


def recv_packet(sock):
    header_len = struct.unpack(">I", recv_exact(sock, 4))[0]
    header = json.loads(recv_exact(sock, header_len).decode("utf-8"))
    size = header.get("size", 0)
    body = recv_exact(sock, size) if size else b""
    return header, body


def save_file(sender, filename, data):
    """Save received bytes into the downloads folder (never overwrite)."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = os.path.basename(filename)
    path = os.path.join(DOWNLOAD_DIR, filename)
    base, ext = os.path.splitext(filename)
    n = 1
    while os.path.exists(path):
        path = os.path.join(DOWNLOAD_DIR, f"{base}_{n}{ext}")
        n += 1
    with open(path, "wb") as f:
        f.write(data)
    print(f"\r[file] received '{filename}' from {sender} ({len(data)} bytes) -> {path}")


# --------------------------------------------------------- receiver thread
def receive_loop(sock):
    """Runs in a separate thread: prints everything the server sends."""
    try:
        while True:
            header, body = recv_packet(sock)
            kind = header.get("type")
            if kind == "msg":
                print(f"\r{header['from']}: {header['text']}")
            elif kind == "info":
                print(f"\r* {header['text']}")
            elif kind == "file":
                save_file(header.get("from", "?"), header.get("name", "file"), body)
            elif kind == "error":
                print(f"\rError: {header['text']}")
                break
    except (ConnectionError, OSError, ValueError, json.JSONDecodeError):
        pass
    print("\rDisconnected from server.")
    sys.stdout.flush()
    os._exit(0)  # stop the whole program (main thread is stuck in input())


# -------------------------------------------------------------------- main
def send_file(sock, path):
    path = path.strip().strip('"').strip("'")
    if not os.path.isfile(path):
        print(f"* File not found: {path}")
        return
    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        print("* File too large (max 50 MB).")
        return
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        print(f"* Cannot read file: {e}")
        return
    # filename + size go first (in the header), then the file bytes (the body)
    send_packet(sock, {"type": "file", "name": os.path.basename(path)}, data)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000

    name = input("Username: ").strip()
    if not name or " " in name:
        print("Username must not be empty or contain spaces.")
        return

    # TCP: SOCK_STREAM = TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # TCP: connect() starts the 3-way handshake with the server
        sock.connect((host, port))
    except OSError as e:
        print(f"Cannot connect to {host}:{port} -> {e}")
        return

    send_packet(sock, {"type": "join", "name": name})
    threading.Thread(target=receive_loop, args=(sock,), daemon=True).start()

    try:
        while True:
            line = input().strip()
            if not line:
                continue
            if line == "/quit":
                send_packet(sock, {"type": "quit"})
                break
            elif line == "/users":
                send_packet(sock, {"type": "users"})
            elif line.startswith("/send"):
                parts = line.split(maxsplit=1)
                if len(parts) < 2:
                    print("* Usage: /send <filename>")
                else:
                    send_file(sock, parts[1])
            elif line.startswith("/"):
                print("* Unknown command. Use /send, /users, /quit")
            else:
                send_packet(sock, {"type": "msg", "text": line})
    except (EOFError, KeyboardInterrupt):
        pass
    except OSError:
        print("Connection lost.")
    finally:
        try:
            sock.close()  # TCP: close the connection
        except OSError:
            pass


if __name__ == "__main__":
    main()
