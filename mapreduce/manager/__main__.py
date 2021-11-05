import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
import pdb
from threading import Thread
from pathlib import Path
from json import JSONDecodeError
# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Manager:
    def __init__(self, port_number, hb_port_number):
        logging.info("Starting manager:%s, %s", port_number, hb_port_number)
        logging.info("Manager:%s, %s PWD %s", port_number, hb_port_number, os.getcwd())
        self.port_number = port_number
        self.hb_port_number = hb_port_number
        self.alive = True
        self.workers = {}
        # This is a fake message to demonstrate pretty printing with logging

        """
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port1": 6001,
            "worker_port2": 6002,
            "worker_pid1": 77811,
            "worker_pid2": 77893,
        }
        logging.debug("Manager:%s, %s received\n%s",
            port_number, hb_port_number, 
            json.dumps(message_dict, indent=2),
        )
        logging.debug("IMPLEMENT ME!")
        """
        cwd = Path.cwd()
        tmp_folder = Path(cwd / 'mapreduce' / 'manager' / 'tmp/')
        if Path.exists(tmp_folder):
            for job in tmp_folder.glob('job-*'):
                job.unlink()
        else:
            Path.mkdir(tmp_folder, parents=True)
        
        # Create threads:
        # breakpoint()
        # logging.debug("Manager:%s, %s", self.port_number, self.hb_port_number)
        udp_thread = Thread(target=self.listen_udp_socket, args=())
        tcp_thread = Thread(target=self.listen_tcp_manager, args=())
        # fault_thread = Thread(target=self.fault_localization args=(self,))
        udp_thread.start()
        tcp_thread.start()
        # fault_thread.start()
        udp_thread.join()
        tcp_thread.join()
        # fault_thread.join()
        # TODO Wait for incoming messages! Ignore invalid messages, 
        # including those that fail JSON decoding. 
        # To ignore these messages use a try/except 
        # when you to try to load the message as shown below
    
    # Thread Specific Functions
    def listen_udp_socket(self):
        # breakpoint()
        # TODO Test UDP SOCKET TO SEE IF IT RECEIVES MESSAGES
        # Create UDP Socket for UDP thread:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.hb_port_number))
            sock.settimeout(1)
            # Receive incoming UDP messages
            while True:
                if self.alive is False: break
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except (JSONDecodeError, TypeError):
                    continue
        logging.debug("Manager:%s Shutting down...", self.port_number) 


    def listen_tcp_manager(self):
        # TODO Test TCP SOCKET TO SEE IF IT RECEIVES MESSAGES
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.port_number))
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
                            logging.debug("Manager:%s timeout", self.port_number)
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
                logging.debug("Manager:%s received %s", self.port_number, message_dict) 
                response = self.generate_response(message_dict)
                # Send response to Worker's TCP
                logging.debug("Manager:%s sent %s", self.port_number, response)
                if response['message_type'] == 'register_ack':
                    # Note: This is making the listening tcp thread do
                    # additional work, perhaps we can create threads
                    # to wait and send the responses with the affilated sockets
                    # that match the worker ports.
                    self.send_tcp_worker(response, response['worker_port'])

                elif response['message_type'] == 'shutdown':
                    for worker in self.workers:
                        self.send_tcp_worker(response, worker)
                    break
            self.alive = False
            logging.debug("Manager:%s Shutting down...", self.port_number) 

                

    def send_tcp_worker(self, response, worker_port):
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect(("localhost", worker_port))
            # send a message
            message = json.dumps(response)
            sock.sendall(message.encode('utf-8'))
            # logging.debug("Manager:%s sent %s", self.port_number, response)

    # TODO Remove code duplication by adding function to utils.py
    def generate_response(self, message_dict):
        response = None
        if message_dict['message_type'] == 'shutdown':
            response = {
                "message_type" : "shutdown"
            }

        elif message_dict['message_type'] == 'register':
            response = {
                "message_type": "register_ack",
                "worker_host": "localhost",
                "worker_port": message_dict['worker_port'],
                "worker_pid" : message_dict['worker_pid']
            }
            self.workers[response['worker_port']] = {
                'port': response['worker_port'],
                'pid': response['worker_pid'],
                'status': 'ready',
                'new-worker': True
            }

        # TODO TEST NEW MANAGER JOB SEND/RESPONSE
        elif message_dict['message_type'] == 'new_manager_job':
            response = {
                "message_type": "new_manager_job",
                "input_directory": '<INPUT STRING>',
                "output_directory": '<INPUT STRING>',
                "mapper_executable": '<INPUT STRING>',
                "reducer_executable": '<INPUT STRING>',
                "num_mappers" : "<INPUT INTEGER>",
                "num_reducers" : "<INPUT INTEGER>"
            }
        return response


    def fault_localization(self):
        time.sleep(10)
        click.echo("Shutting down fault localization...")


@click.command()
@click.argument("port_number", nargs=1, type=int)
@click.argument("hb_port_number", nargs=1, type=int)
def main(port_number, hb_port_number):
    # Init the Manager (Constructor)
    Manager(port_number, hb_port_number)
    # Main Manager Thread Ending
    click.echo("Shutting down manager...")


if __name__ == '__main__':
    main()

