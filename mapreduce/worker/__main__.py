"""worker main."""
import os
import logging
import json
import time
from threading import Thread
# import mapreduce.utils
# import pathlib
from pathlib import Path
import socket
import subprocess
import heapq
import click
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """worker class rise up."""

    def __init__(self, manager_tcp_port, manager_hb_port, worker_port):
        """Initiate worker."""
        self.worker_id = os.getpid()
        self.alive = True
        self.manager_tcp_port = manager_tcp_port
        self.manager_hb_port = manager_hb_port
        self.worker_port = worker_port
        self.status = 'ready'
        logging.info("Starting worker:%s PID: %s", worker_port, self.worker_id)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        tcp_thread = Thread(target=self.listen_tcp_worker, args=())
        tcp_thread.start()

        self.send_register_msg()

        tcp_thread.join()

    def send_register_msg(self):
        """Send register to manager."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))

            # Send registration
            message = json.dumps({
                "message_type": "register",
                "worker_host": "localhost",
                "worker_port": self.worker_port,
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def handle_msg(self, message_dict):
        """Handle manager message, work on task, and return response."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            logging.info("input: %s, output: %s, executable: %s",
                         message_dict['input_files'],
                         message_dict['output_directory'],
                         message_dict['executable'])
            file_input = message_dict["input_files"]
            output_files = []
            for file in file_input:
                file_input_path = Path(file)
                f_name = str(file.split('/')[-1])
                fout_path = Path(message_dict["output_directory"])/f_name
                output_files.append(str(fout_path))
                with open(
                          str(file_input_path),
                          'r',
                          encoding='UTF-8') as i, open(str(fout_path),
                                                       'w',
                                                       encoding='UTF-8') as fi:
                    subprocess.run([message_dict["executable"],
                                   str(file_input_path)], stdin=i, stdout=fi,
                                   text=True, check=True, shell=True)

            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))
            message = json.dumps({
                "message_type": "status",
                "output_files": output_files,
                "status": "finished",
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def handle_sort(self, message_dict):
        """Handle sort and return response to manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            need_sorting = message_dict['input_files']
            # go through each file in need_sorting and sort the file by line.
            # Treat each line as a string.
            for file in need_sorting:
                sorted_file = []
                with open(file, 'r', encoding='UTF-8') as read_f:
                    lines = [line.replace('\n', '') for line in read_f]
                    sorted_file = sorted(lines)
                with open(file, "w", encoding='UTF-8') as write_f:
                    for line in sorted_file:
                        line += '\n'
                        # if not (line == sorted_file[-1]):
                        #     line += '\n'
                        write_f.write(line)

            open_files = []
            for file in need_sorting:
                # need fix
                open_files.append(open(file, encoding='UTF-8'))
            with open(message_dict['output_file'],
                      'w', encoding='UTF-8') as writer:
                for line in heapq.merge(*open_files):
                    writer.write(line)
            for file in open_files:
                file.close()
            sock.connect(("localhost", self.manager_tcp_port))
            message = json.dumps({
                "message_type": "status",
                "output_file": message_dict['output_file'],
                "status": "finished",
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def listen_tcp_worker(self):
        """Listen to manager and generate appropriate response."""
        udp_thread = Thread()
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to
        # automatically be closed when an exception is raised or control
        # flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()

            # Socket accept() and recv() will block for a maximum of
            # 1 second. If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while True:
                # Wait for a connection for 1s.  The socket library
                # avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                # Receive data, one chunk at a time.  If recv() times
                # out before we can read a chunk, then go back to the
                # top of the loop and try again.
                # When the client closes the connection, recv()
                # returns empty data,
                # which breaks out of the loop.
                # We make a simplifying assumption that
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
                response = self.generate_response(message_dict)
                # logging.info("Worker:%s received %s",
                # self.worker_port, response)
                if response['message_type'] == 'register_ack':
                    # Spawn a UDP thread and call the
                    # send_heartbeat funct:
                    # logging.info("Worker:%s forwarding %s",
                    # self.worker_port, response)
                    udp_thread = Thread(target=self.scream_udp_socket,
                                        args=())
                    udp_thread.start()
                if response['message_type'] == 'shutdown':
                    # Kill the UDP thread before ending TCP thread:
                    self.alive = False
                    try:
                        udp_thread.join()
                    except RuntimeError:
                        pass
                    break
                if response['message_type'] == 'new_worker_task':
                    self.handle_msg(response)
                elif response['message_type'] == 'new_sort_task':
                    self.handle_sort(response)

                else:
                    logging.debug("Worker:%s received %s",
                                  self.worker_port, message_dict)
        logging.debug("Worker:%s Shutting down...", self.worker_port)

    def scream_udp_socket(self):
        """Send heartbeat if not dead."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect(("localhost", self.manager_hb_port))
            # Send a heartbeat every 2 seconds:
            # Later, figure out fault tolerance (Once a worker dies)
            while True:
                if self.alive is False:
                    break
                message = json.dumps({
                        "message_type": "heartbeat",
                        "worker_pid": self.worker_id
                    })
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)
        logging.debug("Worker:%s Shutting down...", self.manager_hb_port)

    def generate_response(self, message_dict):
        """Generate response to manager in the appropriate format."""
        response = None
        # logging.info("Worker: %s Received message: %s",
        # self.worker_port, message_dict)
        if message_dict['message_type'] == 'register_ack':
            response = {
                "message_type": "register_ack"
            }
        elif message_dict['message_type'] == 'shutdown':
            response = {
                "message_type": "shutdown"
            }
        elif message_dict['message_type'] == 'new_worker_task':
            response = {
                "message_type": 'new_worker_task',
                "input_files": message_dict['input_files'],
                "executable": message_dict['executable'],
                "output_directory": message_dict['output_directory'],
                "worker_pid": message_dict['worker_pid']
            }
        elif message_dict['message_type'] == 'new_sort_task':
            response = {
                "message_type": "new_sort_task",
                "input_files": message_dict['input_files'],
                "output_file": message_dict['output_file'],
                "worker_pid": message_dict['worker_pid']
                }
        return response


@click.command()
@click.argument("manager_tcp_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_tcp_port, manager_hb_port, worker_port):
    """Initialize Constructor and begin work."""
    Worker(manager_tcp_port, manager_hb_port, worker_port)
    print("shutting down worker...")


if __name__ == '__main__':
    main()
