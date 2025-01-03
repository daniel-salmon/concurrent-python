import logging
import selectors
import socket
from selectors import BaseSelector
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET
from typing import cast

ADDRESS = ("127.0.0.1", 8000)
CHUNK_SIZE = 1024
TIMEOUT = 1.0

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    logging.info(f"Starting server on {ADDRESS}")
    server = socket.socket(AF_INET, SOCK_STREAM)
    server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server.setblocking(False)
    server.bind(ADDRESS)
    server.listen()

    selector = selectors.DefaultSelector()
    selector.register(server, selectors.EVENT_READ)

    listen_and_serve(server, selector, TIMEOUT)


def listen_and_serve(
    server: socket.socket,
    selector: BaseSelector,
    timeout: float,
) -> None:
    while True:
        for key, _ in selector.select():
            sock = cast(socket.socket, key.fileobj)
            if sock == server:
                connection, address = server.accept()
                connection.setblocking(False)
                selector.register(connection, selectors.EVENT_READ)
                logging.info(f"New connection from {address}")
            else:
                data = sock.recv(CHUNK_SIZE)
                sock.sendall(data)


if __name__ == "__main__":
    main()
