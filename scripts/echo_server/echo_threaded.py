import logging
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET

ADDRESS = ("127.0.0.1", 8000)
CHUNK_SIZE = 1024
MAX_WORKERS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

logger.info(f"GIL enabled?: {sys._is_gil_enabled()}")  # type: ignore


def main():
    server = socket.socket(AF_INET, SOCK_STREAM)
    server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server.bind(ADDRESS)
    server.listen()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            connection, address = server.accept()
            logger.info(f"Connection on {address}")
            _ = executor.submit(handle, connection)


def handle(connection: socket.socket) -> None:
    try:
        while True:
            data = read_bytes(connection, CHUNK_SIZE)
            connection.sendall(data)
    except Exception as e:
        logger.exception(e)
    finally:
        connection.close()


def read_bytes(connection: socket.socket, chunk_size: int) -> bytes:
    data = []
    while True:
        buffer = connection.recv(chunk_size)
        data.append(buffer)
        if buffer[-2:] == b"\r\n":
            return b"".join(data)


if __name__ == "__main__":
    main()
