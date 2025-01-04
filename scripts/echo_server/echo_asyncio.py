import asyncio
import logging
import socket
from asyncio import AbstractEventLoop
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET

ADDRESS = ("127.0.0.1", 8000)
CHUNK_SIZE = 1024


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    server = socket.socket(AF_INET, SOCK_STREAM)
    server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server.setblocking(False)
    server.bind(ADDRESS)
    server.listen()

    loop = asyncio.get_event_loop()

    await listen_and_serve(server, loop)


async def listen_and_serve(server: socket.socket, loop: AbstractEventLoop) -> None:
    while True:
        connection, address = await loop.sock_accept(server)
        logger.info(f"Connection on {address}")
        asyncio.create_task(handle(connection, loop))


async def handle(connection: socket.socket, loop: AbstractEventLoop) -> None:
    try:
        while True:
            data = await read_bytes(connection, loop, CHUNK_SIZE)
            await loop.sock_sendall(connection, data)
    except Exception as e:
        logger.exception(e)
    finally:
        connection.close()


async def read_bytes(
    connection: socket.socket,
    loop: AbstractEventLoop,
    chunk_size: int,
) -> bytes:
    data = []
    while True:
        buffer = await loop.sock_recv(connection, chunk_size)
        data.append(buffer)
        if buffer[-2:] == b"\r\n":
            return b"".join(data)


if __name__ == "__main__":
    asyncio.run(main())
