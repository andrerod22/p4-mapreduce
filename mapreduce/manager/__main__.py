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
        self.curr_worker = None
        self.job_ids = 0
        self.tmp_folder = None
        self.busy = False
        self.jobs = []
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
        # TODO DECLARE DEAD WORKER IF MORE THAN 5 PINGS UNRESPONDED
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
        mgr_thread = Thread()
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be closed 
        # when an exception is raised or control flow returns.
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
                        self.workers[message_dict['worker_pid']['status']] = 'ready'
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
                    logging.debug("Shutting down workers: %s", self.workers) 
                    for worker in self.workers:
                        self.send_tcp_worker(response, self.workers[worker]['port'])
                    break
                elif response['message_type'] == 'new_manager_job':
                    self.make_job()
                    # Send the job to an available worker, 
                    # If all workers are busy or manager is busy, place in queue
                    # Busy Manger: Assigning Tasks, Grouping, 
                    # Busy Worker: Processing Job
                    if self.busy:
                        self.jobs.append(response)
                    else:
                        self.jobs.append(response)
                        mgr_thread = Thread(target=self.execute_job, args=())
                        self.execute_job(response)
                    logging.info("Manager:%s new job number %s", self.port_number, self.jobs[self.job_ids])
                    self.job_ids += 1
            self.alive = False
            mgr_thread.join()
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

    def remove_jobs(self, path):
        path = Path(path)
        for item in path.glob('*'):
            if item.is_file():
                item.unlink()
            else:
                self.remove_jobs(item)
        path.rmdir()

    def execute_job(self):
        #Keep thread alive, as long as jobs are pending
        while self.jobs and self.alive:
            '''
                "message_type": "new_manager_job",
                "input_directory": Path(message_dict['input_directory']),
                "output_directory": Path(message_dict['output_directory']),
                "mapper_executable": Path(message_dict['mapper_executable']),
                "reducer_executable": Path(message_dict['reducer_executable']),
                "num_mappers" : int(message_dict['num_mappers']),
                "num_reducers" : int(message_dict['num_reducers'])
            '''
            #Check if Workers are available:
            busy_count = 0
            curr_worker = None
            curr_job = None
            for worker in self.workers:
                if self.workers[worker]['status'] == 'busy':
                    busy_count += 1
                elif self.workers[worker]['status'] == 'ready':
                    self.curr_worker = worker
                    worker['status'] = 'busy'
                    break
            if busy_count == len(self.workers):
                break

            #Begin Map-Reduce Phase:
            self.busy = True
            curr_job = self.jobs.pop(0)

            # TODO: Mapping (Andrew)
            self.map_stage(curr_job)
            
            # TODO: Grouping (N/A)
            self.group_stage()

            # TODO: Reduction (N/A)
            self.reduce_stage()
            logging.debug("Map-Reduction Complete")
        
        logging.debug("Map-Reduction Stopping...") 
        self.busy = False

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
            #if response['worker_port'] not in self.workers:
                #self.workers[response['worker_port']] = {}
            # NOTE: Changed keys from worker_port to worker_pid: 11/7/21
            self.workers[response['worker_pid']] = {
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
        elif message_dict['message_type'] == 'status':
        # TODO: Implement the resposne for status (will look similar to new job)
            response = {}
        return response

    def fault_localization(self):
        time.sleep(10)
        click.echo("Shutting down fault localization...")

    def map_stage(self, curr_job):
        #Andrew's Work
        logging.info("Manager:%s begin map stage", self.port_number)
        self.handle_partioning(curr_job)
        logging.info("Manager:%s end map stage", self.port_number)


    def group_stage(self):
        pass

    def reduce_stage(self):
        pass

    def handle_partioning(self, curr_job):
        #Andrew's Work
        input_dir = curr_job["input_directory"]
        path = Path(input_dir)
        #https://thispointer.com/python-get-list-of-files-in-directory-sorted-by-name/
        input_files = sorted( filter( lambda x: os.path.isfile(os.path.join(input_dir, x)),
                        os.listdir(input_dir) ) )
        partioned = [] #list of list of strings
        for idx, file in enumerate(input_files):
            insertdx = idx % curr_job["num_mappers"] # int 
            if not isinstance(partioned[insertdx], list):
                partioned.insert(insertdx, file)
            else:
                partioned[insertdx].append(file)
        self.map_tasks = partioned

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
