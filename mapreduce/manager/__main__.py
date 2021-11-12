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
        self.curr_job = None
        self.job_ids = 0
        self.tmp_folder = None
        self.handle_partition_done = False
        self.stages = []
        self.jobs = []
        self.tasks = []
        self.sort_dex = 1 # Used for keeping track of sort file index, ex: sorted01, sorted02...
        cwd = Path.cwd()
        # Make future dictionary for all bools
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
                    #elif not self.jobs:
                        #return None
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    for worker in self.workers:
                        if self.workers[worker]['status'] == 'ready': #and self.jobs and self.stages:
                            self.resume_job()
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
                        pid = message_dict['worker_pid']
                        self.workers[pid]['status'] = 'ready'
                        self.resume_job()
                response = self.generate_response(message_dict)
                # Send response to Worker's TCP
                #logging.debug("Manager:%s sent %s", self.port_number, response)
                if response['message_type'] == 'register_ack':
                    self.send_tcp_worker(response, response['worker_port'])

                elif response['message_type'] == 'shutdown':
                    logging.debug("Shutting down workers: %s", self.workers) 
                    for worker in self.workers:
                        self.send_tcp_worker(response, self.workers[worker]['port'])
                    break
                elif response['message_type'] == 'new_manager_job':
                    self.make_job()
                    logging.info("Manager:%s new job number %s", self.port_number, self.job_ids)
                    self.jobs.append(response)
                    if response == self.jobs[0]:
                        logging.info("New job in progress..")
                        self.stages = ['map','group','reduce']
                        self.curr_job = self.jobs[0]
                        self.curr_job['job_id'] = self.job_ids
                        for worker in self.workers:
                            if self.workers[worker]['status'] == 'ready' and self.jobs:
                                self.execute_job()
                                break
                    else:
                        self.resume_job()
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

    def execute_job(self):
        # Begin/Resume Map-Reduce Phase:
        #self.curr_job is the response on new_manager_job. 
        logging.info("Map-Reduction Starting...")
        if self.stages[0] == 'map':
            if not self.handle_partition_done:
                logging.info("Manager:%s begin map stage", self.port_number)
                self.handle_partioning(self.curr_job['num_mappers'])
            logging.info("Tasks Left: %s", self.tasks)
            self.map_stage(self.curr_job)
            if not self.tasks: self.handle_partition_done = False
        elif self.stages[0] == 'group':
            if not self.handle_partition_done:
                logging.info("Manager:%s end map stage", self.port_number)
                logging.info("Manager:%s begin group stage", self.port_number)
                self.sort_partition(self.curr_job)
            if self.tasks:
                self.group_stage(self.curr_job)
            elif not self.tasks:
                self.prep_reduce(self.curr_job)
                self.sort_partition_done = False
                logging.info("Manager:%s end group stage", self.port_number)
        elif self.stages[0] == 'reduce':
            if not self.handle_partition_done:
                self.sort_dex = 1 #reset sortdex for reuse
                self.handle_partioning(self.curr_job['num_reducers'])
            self.reduce_stage(self.curr_job)

    def resume_job(self):
        if not self.tasks:
            # Check if any workers, died and reassign tasks:
            #if self.check_for_deaths():
                #self.tasks.append(self.get_dead_tasks())
            #else:
            if self.stages:
                logging.debug("Leaving: %s", self.stages[0])
                self.stages.pop(0)
                self.handle_partition_done = False
                if not self.stages:
                    # Job is done, check queue for next job:
                    logging.debug("Job is done!")
                    if self.jobs:
                        self.jobs.pop(0)
                        if self.jobs: 
                            self.curr_job = self.jobs[0]
                            self.curr_job['job_id'] = self.job_ids
        if self.jobs:
            logging.debug("Resuming job...")
            self.execute_job()

    def send_tcp_worker(self, response, worker_port):
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            
            sock.connect(("localhost", worker_port))
            # send a message
            message = json.dumps(response)
            sock.sendall(message.encode('utf-8'))
            logging.info("Manager:%s sent %s to %s", self.port_number, response, worker_port)

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
                #'task_number': -1
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

    def handle_partioning(self, num): #num is num_mapper
        self.handle_partition_done = True
        input_dir = self.curr_job["input_directory"]
        # https://thispointer.com/python-get-list-of-files-in-directory-sorted-by-name/
        input_files = sorted( filter( lambda x: os.path.isfile(os.path.join(input_dir, x)),
                        os.listdir(input_dir) ) )
        input_files = [self.curr_job["input_directory"] + '/' + file for file in input_files]
        partioned = []
        for i in range(0, num):
            indx = i % num
            tasks = [input_files[x] for x in range(len(input_files)) if x % num == indx]
            partioned.append(tasks)
        self.tasks = partioned

    def map_stage(self, curr_job):
        for _ in range(len(self.tasks)): #was originally a for loop. I think while loop is correct. 
            logging.info("tasks left %s ", str(len(self.tasks)))
            busy_count = 0
            for worker in self.workers:
                if self.workers[worker]['status'] == 'ready': 
                    logging.info("current job: " + str(curr_job['job_id']))
                    job_id = 'job-' + str(curr_job['job_id']) + '/'
                    tmpPath = Path('tmp/')
                    response = {
                        "message_type": "new_worker_task",
                        "input_files": self.tasks[0],
                        "executable": curr_job['mapper_executable'],
                        "output_directory": str(Path(tmpPath / job_id / 'mapper-output/')),
                        "worker_pid": self.workers[worker]['pid']
                    }
                    self.send_tcp_worker(response, self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.tasks[0]
                    self.tasks.pop(0)
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)
                    busy_count += 1
            if busy_count == len(self.workers):
                logging.info("All workers busy!")
                return None

    def sort_partition(self, curr_job):
        #get list of input files, ex: "tmp/job-0/mapper-output/file01"
        job_id = 'job-' + str(curr_job['job_id']) + '/'
        tmpPath = Path('tmp/')
        output_direc = Path(tmpPath / job_id / 'mapper-output/')
        map_files = [str(e) for e in output_direc.iterdir() if e.is_file()]

        #round robin the map_output into self.tasks for each worker.
        partitioned = []
        num_workers = len(self.workers)
        if len(map_files) >= num_workers: # If the number of files is greater than the number of workers. 
            for i in range(0, num_workers):
                indx = i % num_workers
                tasks = [map_files[x] for x in range(len(map_files)) if x % num_workers == indx]
                partitioned.append(tasks)
        else: #if number of workers is more than number of files. 
            for file in map_files:
                partitioned.append([file])
        self.tasks = partitioned
        logging.debug("Tasks in sort_partition: %s", self.tasks)
        self.sort_partition_done = True

    def group_stage(self, curr_job):
        ### Only handles the sorting portion. 
        for _ in range(len(self.tasks)):
            busy_count = 0
            for worker in self.workers:
                if self.workers[worker]['status'] == 'ready': 
                    job_id = Path('job-' + str(curr_job['job_id'])) #+ '/'
                    #tmpPath = Path('tmp/')
                    sort_num = "0" + str(self.sort_dex) if self.sort_dex < 10 else str(self.sort_dex)
                    sort_path = "/sorted" + sort_num
                    grouper_file_path = Path('tmp'/ job_id / 'grouper-output'/ sort_path) 
                    response = {
                        "message_type": "new_sort_task",
                        "input_files": self.tasks[0],
                        "output_file": str(grouper_file_path),
                        "worker_pid": self.workers[worker]['pid']
                    }
                    logging.debug("Response Output File: %s", response['output_file'])
                    self.send_tcp_worker(response, self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.tasks[0]
                    self.tasks.pop(0)
                    self.sort_dex += 1
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)
                    busy_count += 1
            if busy_count == len(self.workers):
                logging.info("All workers busy!")
                return None
    
    def prep_reduce(self, curr_job):
        # Get all important path info:
        job_id = Path('job-' + str(curr_job['job_id']))
        grouper_folder = Path('tmp' / job_id / 'grouper-output/')
        extracted_file = []
        # Loop through all sorted files:
        for sorted_file in grouper_folder.glob('sorted*'):
            # Open file 
            with open(str(sorted_file), 'r') as s:

                # Store file contents into list of list of strings
                extracted_file = [line for line in s]
        
        # Iterate through list
        #for line in extracted_file:
            #indx = line + 1 % curr_job['num_reducers']
            #Make index for each num_reducer
        
            for i in range(0, curr_job['num_reducers']):
                indx = i % curr_job['num_reducers']
                extract_lines = [extracted_file[0][line] for line in extracted_file[0] if line % curr_job['num_reducers'] == indx]
                #reduce_tasks.append(extract_lines)
                with open("reduce" + indx + 1, 'a') as f:
                    for words in extract_lines:
                        f.write(words)

            #reduce_tasks = []
            #Append Task if new
            #Insert if element exists already




    def reduce_stage(self, curr_job):
        logging.info("Manager:%s begin reduce stage", self.port_number)
        self.stages.pop(0)
        """
        for i in range(len(self.tasks)):
        # logging.info("On task: %s", i)
            busy_count = 0
            for worker in self.workers:
                # logging.info("On worker:%s",worker)
                if self.workers[worker]['status'] == 'ready': 
                    response = {
                        "message_type": "new_worker_task",
                        "input_files": self.tasks[i],
                        "executable": curr_job['mapper_executable'],
                        "output_directory": curr_job['full_output_directory'] if curr_job['full_output_directory'] else curr_job['output_directory'],
                        "worker_pid": self.workers[worker]['pid']
                    }
                    # logging.info("Tasks:%s ", self.map_tasks)
                    # logging.info("Manager: %s Sending: %s", self.port_number, response)
                    self.send_tcp_worker(response, self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.tasks[i]
                    self.tasks.pop(0)
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)
                    busy_count += 1
                    #continue
            #return
            #logging.info("stuck")
            if busy_count == len(self.workers):
                logging.debug("All workers busy!")
                return None
                #else: Worker is dead!
                #elif (self.workers[worker]['status'] == 'dead'
                    #and self.workers[worker]['task'] in self.map_tasks):
                    # Reassign task to another worker if current worker dead:
                    # Might need the index for self.map_tasks
                    # Which is: [self.workers[worker]['task_number']]
                    #pass
        """
        logging.info("Manager:%s end reduce stage", self.port_number)

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