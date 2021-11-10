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
        self.curr_worker = None
        self.curr_job = None
        self.job_ids = 0
        self.tmp_folder = None
        self.handle_partition_done = False
        self.jobs = []
        self.map_tasks = []
        self.reduce_tasks = []
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
        mgr_thread = Thread(target=self.mgr_thread_handle, args=())

        # fault_thread = Thread(target=self.fault_localization args=(self,))
        udp_thread.start()
        tcp_thread.start()
        mgr_thread.start()
        # fault_thread.start()
        udp_thread.join()
        tcp_thread.join()
        mgr_thread.join()
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
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

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
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                logging.debug("Manager:%s received %s", self.port_number, message_dict)
                if message_dict['message_type'] == 'status':
                    if message_dict['status'] == 'finished':
                        pid = message_dict['worker_pid']
                        logging.info("Worker %s is finished", pid)
                        self.workers[pid]['status'] = 'ready'
                response = self.generate_response(message_dict)
                if response['message_type'] == 'register_ack':
                    self.send_tcp_worker(response, response['worker_port'])

                elif response['message_type'] == 'shutdown':
                    logging.debug("Shutting down workers: %s", self.workers) 
                    for worker in self.workers:
                        self.send_tcp_worker(response, self.workers[worker]['port'])
                    break
                elif response['message_type'] == 'new_manager_job':
                    #if all workers are busy then add to job queue. 
                    #doesn't handle when new job
                    breakpoint()
                    self.make_job()
                    self.jobs.append(response)
                    if self.curr_job is None:
                        self.curr_job = response
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
    
    def remove_jobs(self, path):
        path = Path(path)
        for item in path.glob('*'):
            if item.is_file():
                item.unlink()
            else:
                self.remove_jobs(item)
        path.rmdir()

    def mgr_thread_handle(self):
        #break out when all map_reduce tasks are finished. 
        # pop from job queue here.
        while True: 
            map = False
            group = False
            reduce = False
            while not (map and group and reduce):
                if self.curr_job is not None: #run while loop (keep checking) until we have a job. 
                    self.map_stage(self.curr_job)
                    map = True
                    self.group_stage()
                    group = True
                    self.reduce_stage()
                    reduce = True
            self.jobs.pop(0)
            if len(self.jobs) > 0: self.curr_job = self.jobs[0] #if we have more in the job queue. 
            else: self.curr_job = None
            logging.info("Map Reduce Complete!")
            if not self.alive: break

    def send_tcp_worker(self, response, worker_port):
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            
            sock.connect(("localhost", worker_port))
            # send a message
            message = json.dumps(response)
            sock.sendall(message.encode('utf-8'))
            # logging.info("Manager:%s sent %s to %s", self.port_number, response, worker_port)

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
                'task': '',
                'task_type': '',
                'task_number': -1
                #'new-worker': True
            }

        # TODO TEST NEW MANAGER JOB SEND/RESPONSE
        elif message_dict['message_type'] == 'new_manager_job':
            response = {
                "message_type": "new_manager_job",
                "input_directory": message_dict['input_directory'],
                "output_directory": message_dict['output_directory'],
                "mapper_executable": message_dict['mapper_executable'],
                "reducer_executable": message_dict['reducer_executable'],
                "num_mappers" : int(message_dict['num_mappers']),
                "num_reducers" : int(message_dict['num_reducers']),
                "full_output_directory": "tmp/job-{}/mapper-output".format(self.job_ids) if message_dict['output_directory'] == 'output' else None
            }
        elif message_dict['message_type'] == 'status':
        # TODO: Implement the resposne for status (will look similar to new job)
            response = {
                'message_type' : 'status'
            }
        return response

    def fault_localization(self):
        time.sleep(10)
        click.echo("Shutting down fault localization...")

    def map_stage(self, curr_job):
        """
        Go through map_tasks and distributes to each worker. 
        """
        #Andrew's Work
        logging.info("Manager:%s begin map stage", self.port_number)
        self.handle_partioning(curr_job)
        # logging.info(f"Number of workers {len(self.workers)}")
        while self.map_tasks:
            logging.info("TASKS REMAINING: %s", len(self.map_tasks))
            busy_count = 0
            for worker in self.workers:
                # logging.info("On worker:%s",worker)
                if self.workers[worker]['status'] == 'ready': 
                    logging.info("Assigning to work to worker %s", worker)
                    response = {
                        "message_type": "new_worker_task",
                        "input_files": self.map_tasks[0],
                        "executable": curr_job['mapper_executable'],
                        "output_directory": curr_job['full_output_directory'] if curr_job['full_output_directory'] else curr_job['output_directory'],
                        "worker_pid": self.workers[worker]['pid']
                    }
                    self.send_tcp_worker(response, self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.map_tasks[0]
                    self.workers[worker]['task_type'] = 'map'
                    self.map_tasks.pop(0)
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)
                    busy_count += 1
            if busy_count == len(self.workers):
                logging.info("All workers busy!")
        #Run through to tell workers to see if any are dead and reassign their tasks. (implement later)
        logging.info("Manager:%s end map stage", self.port_number)

    def group_stage(self):
        pass

    def reduce_stage(self):
        pass

    def handle_partioning(self, curr_job):
        self.handle_partition_done = True
        input_dir = curr_job["input_directory"]
        #https://thispointer.com/python-get-list-of-files-in-directory-sorted-by-name/
        input_files = sorted( filter( lambda x: os.path.isfile(os.path.join(input_dir, x)),
                        os.listdir(input_dir) ) )
        input_files = [curr_job['input_directory'] + '/' + file for file in input_files]
        partioned = []
        for i in range(0, curr_job['num_mappers']):
            indx = i % curr_job["num_mappers"]
            tasks = [input_files[x] for x in range(len(input_files)) if x % curr_job['num_mappers'] == indx]
            partioned.append(tasks)
        self.map_tasks = partioned
        # logging.info("Maps Left: %s", self.map_tasks)
        # logging.info("num_mappers: %s", curr_job["num_mappers"])
        #logging.info("Map Tasks: %s", self.map_tasks)
 


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

#Logic for implemented pop AFTER workers send "finish" status:
                        #if self.workers[pid]['task_type'] == 'map':
                            #for task in self.map_tasks:
                                #if self.workers[pid]['task'] == self.map_tasks[task]:
                                    #self.map_tasks.pop(task)
                        #else:
                            #self.reduce_tasks.pop(self.workers[pid]['task_number'])
                        #logging.debug("Mapping Tasks Left: %s", self.map_tasks)


#Old logic for partitioning:
            #logging.debug("insertdx: %s", insertdx) 
            #if not isinstance(partioned, list):
            #file_path = curr_job['input_directory'] + '/' + str(file)
            #partioned.append(str(file_path))

            #if len(partioned) <= insertdx:
                #partioned.insert(insertdx, str(file_path))
            #else:
                #prev = partioned[insertdx]
                #partioned.pop(insertdx)
                #partioned.insert(insertdx, file_path)
                #new_partition = partioned[::insertdx]
                #partioned.pop(insertdx)
                #partioned.insert(insertdx, new_partition)