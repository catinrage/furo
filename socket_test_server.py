#!/usr/bin/env python3
import argparse
import os
import socket
import threading


def recv_line(conn: socket.socket, limit: int = 8192) -> bytes:
    data = bytearray()
    while len(data) < limit:
        chunk = conn.recv(1)
        if not chunk:
            raise ConnectionError("connection closed while reading line")
        if chunk == b"\n":
            return bytes(data).rstrip(b"\r")
        data.extend(chunk)
    raise ValueError("line too long")


def recv_exact(conn: socket.socket, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            raise ConnectionError("connection closed while reading payload")
        data.extend(chunk)
    return bytes(data)


def send_all(conn: socket.socket, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        sent = conn.send(view)
        if sent <= 0:
            raise ConnectionError("connection closed while sending")
        view = view[sent:]


def handle_client(conn: socket.socket, addr, pull_chunk: bytes) -> None:
    conn.settimeout(30)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    print(f"[+] client {addr[0]}:{addr[1]}")
    try:
        while True:
            line = recv_line(conn).decode("utf-8", "replace")
            parts = line.split()
            if not parts:
                continue

            cmd = parts[0].upper()
            if cmd == "PING":
                send_all(conn, b"PONG\n")
                continue

            if cmd in {"PUSH", "PULL", "ECHO"}:
                if len(parts) != 2:
                    send_all(conn, b"ERR bad-args\n")
                    continue
                try:
                    size = int(parts[1])
                except ValueError:
                    send_all(conn, b"ERR bad-size\n")
                    continue
                if size < 0 or size > 128 * 1024 * 1024:
                    send_all(conn, b"ERR size-range\n")
                    continue
            else:
                send_all(conn, b"ERR unknown-cmd\n")
                continue

            if cmd == "PUSH":
                send_all(conn, b"READY\n")
                _ = recv_exact(conn, size)
                send_all(conn, b"OK " + str(size).encode() + b"\n")
                continue

            if cmd == "PULL":
                send_all(conn, b"DATA " + str(size).encode() + b"\n")
                remaining = size
                while remaining > 0:
                    chunk = pull_chunk[: min(remaining, len(pull_chunk))]
                    send_all(conn, chunk)
                    remaining -= len(chunk)
                send_all(conn, b"OK " + str(size).encode() + b"\n")
                continue

            if cmd == "ECHO":
                send_all(conn, b"READY\n")
                data = recv_exact(conn, size)
                send_all(conn, b"DATA " + str(size).encode() + b"\n")
                send_all(conn, data)
                send_all(conn, b"OK " + str(size).encode() + b"\n")
                continue
    except Exception as exc:
        print(f"[-] client {addr[0]}:{addr[1]} disconnected: {exc}")
    finally:
        try:
            conn.close()
        except OSError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple TCP socket test server for PHP relay experiments")
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=19090)
    parser.add_argument("--chunk-size", type=int, default=65536)
    args = parser.parse_args()

    pull_chunk = os.urandom(max(1024, args.chunk_size))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server.bind((args.listen, args.port))
        server.listen(128)
        print(f"[*] listening on {args.listen}:{args.port}")

        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr, pull_chunk), daemon=True)
            thread.start()


if __name__ == "__main__":
    main()
