import os
import logging
import json
import time
from threading import Thread
import click
import mapreduce.utils
import pathlib
import socket
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, manager_tcp_port, manager_hb_port, worker_port):
        self.worker_id = -1
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": os.getpid()
        }
        logging.debug(
            "Worker:%s received\n%s",
            worker_port,
            json.dumps(message_dict, indent=2),
        )
        logging.debug("IMPLEMENT ME!")
        self.fetch_id()
        tcp_thread = Thread(target=self.tcp_socket, args=(worker_port,))
        tcp_thread.start()
        # TODO: Send the register message to the Manager. 
        # Make sure you are listening before sending this message.
        # TODO: Upon receiving the register_ack message, create a new thread 
        # which will be responsible for sending heartbeat messages to the Manager.
        tcp_thread.join()

    def fetch_id(self):
        self.worker_id = os.getpid()
    def tcp_socket(self, worker_port):
        # TODO Test TCP SOCKET TO SEE IF IT RECEIVES MESSAGES
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("TCP-Worker", worker_port))
            sock.listen()

            # Socket accept() and recv() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while True:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                # Receive data, one chunk at a time.  If recv() times out before we can
                # read a chunk, then go back to the top of the loop and try again.
                # When the client closes the connection, recv() returns empty data,
                # which breaks out of the loop.  We make a simplifying assumption that
                # the client will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)




@click.command()
@click.argument("manager_tcp_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_tcp_port, manager_hb_port, worker_port):
    Worker(manager_tcp_port, manager_hb_port, worker_port)
    print("shutting down worker...")

if __name__ == '__main__':
    main()
