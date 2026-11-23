# TCP Chat & File Share

A multi-client chat and file sharing app written in Python using only the standard library (`socket`, `threading`).

## Features
- Many clients chat at the same time (the server uses one thread per client)
- Broadcast chat messages to everyone
- Send files to all other users with `/send <filename>`
- Connect/disconnect logs on the server
- Error handling: client disconnects, file not found, duplicate usernames, file too large (50 MB limit)

## How to run
Requires Python 3.8+. No extra packages.

1. Start the server (terminal 1):
   ```
   python server.py
   ```
2. Start clients (terminal 2, terminal 3, ...):
   ```
   python client.py
   ```
   To connect from another computer: `python client.py <server_ip> 5000`
3. Type a username, then start chatting.

## Commands
| Command | What it does |
|---|---|
| `/send <filename>` | Sends a file to all other users |
| `/users` | Shows who is online |
| `/quit` | Leaves the chat |

Received files are saved into the `downloads/` folder next to `client.py`.

## How TCP works in this project
TCP is a connection-based protocol that gives a **reliable, ordered byte stream**: data arrives complete, in order, with no loss or corruption. This is why it fits a chat and file transfer app.

- **Server**: `socket(SOCK_STREAM)` creates a TCP socket, `bind()` and `listen()` wait for connections, and `accept()` completes the 3-way handshake with a client and returns a new socket for that client. Each client gets its own thread.
- **Client**: `connect()` starts the 3-way handshake (SYN, SYN-ACK, ACK) with the server.
- **Sending / receiving**: `sendall()` and `recv()` move bytes over the connection. A closed connection makes `recv()` return empty bytes, which is how we detect disconnects.
- **Message framing**: TCP is a stream and has no message boundaries, so every packet is sent as `[4-byte header length][JSON header][optional body]`. For a file, the header holds the filename and size, and the body holds the file bytes. The receiver reads exactly that many bytes.
- **Closing**: `close()` ends the connection.

## Files
- `server.py` - the chat server
- `client.py` - the chat client
