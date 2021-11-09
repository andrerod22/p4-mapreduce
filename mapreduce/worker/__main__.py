import os
import logging
import json
import time
from threading import Thread
import click
import mapreduce.utils
import pathlib
from pathlib import Path
import socket
import subprocess
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, manager_tcp_port, manager_hb_port, worker_port):
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
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))

            # Send registration
            message = json.dumps({
                "message_type": "register",
                "worker_host" : "localhost",
                "worker_port" : self.worker_port,
                "worker_pid" : self.worker_id
                })
            sock.sendall(message.encode('utf-8'))


    def handle_msg(self, message_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            logging.info("input: %s, output: %s, executable: %s", message_dict['input_files'], message_dict['output_directory'], message_dict['executable'])
            #outFileName = fileInput.replace("tests/testdata/input_small/", "")
            fileInput = message_dict["input_files"]
            output_files = []
            for file in fileInput:
                file_input_path = Path(file)
                file_name = str(file.split('/')[-1])
                logging.info("file_name: %s", file_name)
                #file_output_path =  # + outFileName
                file_output_path = Path(message_dict["output_directory"]) / file_name
                output_files.append(str(file_output_path))
                #with open(file_input_path, 'r') as inFile, (file_output_path, 'w') as out_file:
                #input_obj = open(file_input_path, 'r')
                #TODO FINISH THIS: CURRENTLY EXITING WITH 1 NOT SUCESSFUL
                #p1 = subprocess.run(['cat', str(file_input_path)], capture_output=True, text=True)
                
                with open(str(file_input_path), 'r') as i, open(str(file_output_path), 'w') as f:
                    subprocess.run([message_dict["executable"], str(file_input_path)], stdin=i, stdout=f, text=True, check=True)
                #input_obj.close()
            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))
            #out_file_string = "[ " + file_output + " ]"
            # Send registration
            message = json.dumps({
                "message_type": "status",
                "output_files" : output_files,
                "status": "finished",
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def handle_grouping(self, message_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))

            # Send registration
            message = json.dumps({
                "message_type": "status",
                "output_file" : "",
                "status": "finished",
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def handle_reduce(self, message_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the server
            sock.connect(("localhost", self.manager_tcp_port))

            # Send registration
            message = json.dumps({
                "message_type": "status",
                "output_files" : "",
                "status": "finished",
                "worker_pid": self.worker_id
                })
            sock.sendall(message.encode('utf-8'))

    def listen_tcp_worker(self):
        udp_thread = Thread()
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to 
        # automatically be closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
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
                response = self.generate_response(message_dict)
                logging.info("Worker:%s received %s", self.worker_port, response)
                if response['message_type'] == 'register_ack':
                    # Spawn a UDP thread and call the send_heartbeat funct:
                    logging.info("Worker:%s forwarding %s", self.worker_port, response)
                    udp_thread = Thread(target=self.scream_udp_socket, args=())
                    udp_thread.start()
                if response['message_type'] == 'shutdown':
                    # Kill the UDP thread before ending TCP thread:
                    self.alive = False
                    try: udp_thread.join()
                    except(RuntimeError):
                        pass
                    break
                elif response['message_type'] == 'new_worker_task':
                    self.handle_msg(response)

                else:
                    logging.debug("Worker:%s received %s", self.worker_port, message_dict)
                # Send response
        logging.debug("Worker:%s Shutting down...", self.worker_port)


    def scream_udp_socket(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect(("localhost", self.manager_hb_port))
            # Send a heartbeat every 2 seconds:
            # Later, figure out fault tolerance (Once a worker dies)
            while True:
                if self.alive is False: break
                message = json.dumps({
                        "message_type": "heartbeat",
                        "worker_pid": self.worker_id
                    })
                sock.sendall(message.encode('utf-8'))
        logging.debug("Worker:%s Shutting down...", self.manager_hb_port) 


    def generate_response(self, message_dict):
        response = None
        logging.info("Worker: %s Received message: %s", self.worker_port, message_dict)
        if message_dict['message_type'] == 'register_ack':
            response = {
                "message_type" : "register_ack"
            }
        elif message_dict['message_type'] == 'shutdown':
            response = {
                "message_type" : "shutdown"
            }
        elif message_dict['message_type'] == 'new_worker_task':
            response = {
                "message_type": 'new_worker_task',
                "input_files": message_dict['input_files'],
                "executable": message_dict['executable'],
                "output_directory": message_dict['output_directory'],
                "worker_pid": message_dict['worker_pid']
            }
        return response





@click.command()
@click.argument("manager_tcp_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_tcp_port, manager_hb_port, worker_port):
    Worker(manager_tcp_port, manager_hb_port, worker_port)
    print("shutting down worker...")

if __name__ == '__main__':
    main()


"""
# message = json.dumps(response)
# sock.sendall(message.encode('utf-8'))
"""