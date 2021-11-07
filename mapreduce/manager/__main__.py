import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
import pdb
from threading import Thread
from queue import Queue
from pathlib import Path
from json import JSONDecodeError
from collections import defaultdict
# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Manager:
    def __init__(self, port_number, hb_port_number):
        logging.info("Starting manager:%s, %s", port_number, hb_port_number)
        logging.info("Manager:%s, %s PWD %s", port_number, hb_port_number, os.getcwd())
        self.port_number = port_number
        self.hb_port_number = hb_port_number
        self.alive = True
        self.workers = defaultdict(dict)
        self.job_ids = 0
        self.tmp_folder = None
        self.jobs_waiting = Queue(maxsize=0)
        self.curr_job = None
        self.map_tasks = []
        cwd = Path.cwd()
        #tmp_folder = Path(cwd / 'mapreduce' / 'manager' / 'tmp/')
        tmp_folder = Path(cwd / 'tmp/')
        try: Path.mkdir(tmp_folder, parents=True)
        except(FileExistsError):
            for job in tmp_folder.glob('job-*'):
                self.remove_jobs(job)
        self.tmp_folder = tmp_folder

        # Create threads:
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
                if message_dict['message_type'] == 'status':
                    if message_dict['status'] == 'finished':
                        self.workers[message_dict['worker_port']]['status'] = 'ready'
                        # If Queue not Empty, pop and prepare job to be served later
                        # OPTIONAL: Put this code into the generate response function!
                        # if not self.jobs_waiting.empty(): #WARNING: race condition can cause queue to increase in size before empty() is called. 
                        #     self.curr_job = self.jobs_waiting.get()
                        # else:
                        #     self.curr_job = None #set it back to None if we currently don't have a job. 
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
                elif response['message_type'] == 'new_manager_job':
                    self.make_job()
                    if self.curr_job is not None: #if manager is busy. 
                        self.jobs_waiting.put(response)
                    else:
                        self.map_stage()
                    logging.info("Manager:%s new job number %s", self.port_number, self.job_ids)
                    self.job_ids += 1
            self.alive = False
            logging.debug("Manager:%s Shutting down...", self.port_number) 

    def make_job(self):
        new_job = 'job-' + str(self.job_ids) + '/'
        folders = (
            Path(self.tmp_folder / new_job),
            Path(self.tmp_folder / new_job / 'mapper-output/'),
            Path(self.tmp_folder / new_job / 'grouper-output/'),
            Path(self.tmp_folder / new_job / 'reducer-output/')
        )
        for folder in folders:
            Path.mkdir(folder, parents=True)

    def serve_job(self, response, worker):
        self.workers[worker['port']]['status'] = 'busy'
        #should handle multiple types of jobs of mapReduce 
        #Implement, serve job to worker. 
    
    def map_stage(self, response):
        logging.info("Manager:%s begin map stage", self.port_number)
        self.handle_partioning(response)
        busy_count = 0
        for worker in self.workers:
            if worker['status'] == 'busy':
                busy_count += 1
            elif worker['status'] == 'ready':
                self.serve_job(response, worker)
        if busy_count == len(self.workers):
            self.jobs_waiting.put(response)

        logging.info("Manager:%s end map stage", self.port_number)

    def handle_partioning(self, response):
        input_dir = response["input_directory"]
        path = Path(input_dir)
        #https://thispointer.com/python-get-list-of-files-in-directory-sorted-by-name/
        input_files = sorted( filter( lambda x: os.path.isfile(os.path.join(input_dir, x)),
                        os.listdir(input_dir) ) )
        partioned = [] #list of list of strings
        for idx, file in enumerate(input_files):
            insertdx = idx % response["num_mappers"] # int 
            if not isinstance(partioned[insertdx], list):
                partioned.insert(insertdx, file)
            else:
                partioned[insertdx].append(file)
        self.map_tasks = partioned



    def remove_jobs(self, path):
        path = Path(path)
        for item in path.glob('*'):
            if item.is_file():
                item.unlink()
            else:
                self.remove_jobs(item)
        path.rmdir()

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
                "input_directory": Path(message_dict['input_directory']),
                "output_directory": Path(message_dict['output_directory']),
                "mapper_executable": Path(message_dict['mapper_executable']),
                "reducer_executable": Path(message_dict['reducer_executable']),
                "num_mappers" : int(message_dict['num_mappers']),
                "num_reducers" : int(message_dict['num_reducers'])
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

"""
Error Handling Code for failed socket sending:
    except socket.error as err:
        print("Failed to send job to manager.")
        print(err)

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
