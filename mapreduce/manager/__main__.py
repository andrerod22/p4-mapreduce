import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
import pdb
import heapq
from threading import Thread
from pathlib import Path
import shutil
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
        #self.pending_responses = []
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
        fault_thread = Thread(target=self.fault_localization,args=())
        udp_thread.start()
        tcp_thread.start()
        fault_thread.start()
        udp_thread.join()
        tcp_thread.join()
        fault_thread.join()
    
    # Thread Specific Functions
    def listen_udp_socket(self):
        # TODO DECLARE DEAD WORKER IF MORE THAN 5 PINGS, meaning it doesn't send 5 heartbeats (10s). 
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
                #if we receive a heartbeat restore the timer. 
                if self.workers[message_dict['worker_pid']]['status'] != 'dead':
                    self.workers[message_dict['worker_pid']]['timer'] = 10

        
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
            sock.settimeout(1)
            while True:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    for worker in self.workers:
                        if self.workers[worker]['status'] == 'ready':
                            self.resume_job()
                    continue
                # print("Connection from", address[0])

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

                if response['message_type'] == 'register_ack':
                    self.send_tcp_worker(response, response['worker_port'])
                    # check if there is any work for this worker to do. For manager_09
                    if self.tasks:
                        self.resume_job()

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
                        # logging.info("New job in progress..")
                        self.stages = ['map','group','reduce']
                        self.curr_job = self.jobs[0]
                        self.curr_job['job_id'] = self.job_ids
                        self.handle_partioning(self.curr_job['num_mappers'])
                        self.handle_partition_done = True
                        # ready_count = 0
                        for worker in self.workers:
                            if self.workers[worker]['status'] == 'ready' and self.jobs:
                                self.execute_job()
                                break
                        # if self.jobs and ready_count == len(self.workers):
                            # self.execute_job()
                            # if self.workers[worker]['status'] == 'ready' and self.jobs:
                                # self.execute_job()
                                # break
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
        if self.stages[0] == 'map':
            if not self.handle_partition_done:
                self.handle_partioning(self.curr_job['num_mappers'])
            self.mapreduce_stage(self.curr_job, 'mapper')
        elif self.stages[0] == 'group':
            if not self.handle_partition_done:
                logging.info("Manager:%s end map stage", self.port_number)
                logging.info("Manager:%s begin group stage", self.port_number)
                self.sort_partition(self.curr_job)
            self.group_stage(self.curr_job)
        elif self.stages[0] == 'reduce':
            if not self.handle_partition_done:
                logging.info("Manager:%s end group stage", self.port_number)
                logging.info("Manager:%s begin reduce stage", self.port_number)
                self.prep_reduce(self.curr_job)
            self.mapreduce_stage(self.curr_job, 'reducer')
            if not self.tasks:
                logging.info("Manager:%s end reduce stage", self.port_number)


    def resume_job(self):
        #if self.stages:
            #logging.info("%s tasks left: %s", self.stages[0], self.tasks)
        if not self.tasks:
            # Make sure all workers are ready before moving to next stage
            ready_count = 0
            for worker in self.workers:
                if self.workers[worker]['status'] == 'ready':
                    ready_count += 1
            # Check if any workers, died and reassign tasks:
            #if self.check_for_deaths():
                #self.tasks.append(self.get_dead_tasks())
            #else:
            if self.stages and ready_count == len(self.workers):
                logging.debug("Leaving: %s", self.stages[0])
                self.stages.pop(0)
                self.handle_partition_done = False
                if not self.stages:
                    # Job is done, check queue for next job:
                    logging.debug("Job is done!")
                    self.generate_output()
                    if self.jobs:
                        prev_job = self.jobs.pop(0)
                        if self.jobs:
                            self.stages = ['map','group','reduce'] 
                            self.curr_job = self.jobs[0]
                            #logging.info("JOB_ID IN RESUME_JOB:")
                            self.curr_job['job_id'] = prev_job['job_id'] + 1
                            self.sort_dex = 1
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
            #if len(self.workers) != 2:
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

            self.workers[response['worker_pid']] = {
                'port': response['worker_port'],
                'pid': response['worker_pid'],
                'status': 'ready',
                'task': '',
            }

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
        # logging.info("MAP TASKS GIVEN: %s and num_mapper: %s", self.tasks, num)

    def mapreduce_stage(self, curr_job, stage):
        busy_count = 0
        for worker in self.workers:
            if self.tasks and self.workers[worker]['status'] == 'ready': 
                # logging.info("current job: " + str(curr_job['job_id']))
                job_id = 'job-' + str(curr_job['job_id']) + '/'
                tmpPath = Path('tmp/')
                output_folder = stage + '-output/'
                
                response = {
                    "message_type": "new_worker_task",
                    "input_files": self.tasks[0],
                    "executable": curr_job[stage + '_executable'],
                    "output_directory": str(Path(tmpPath / job_id / output_folder)),
                    "worker_pid": self.workers[worker]['pid']
                }
                #logging.info("OUTPUT_DIRECTORY IN MAPREDUCE STAGE: %s", str(Path(tmpPath / job_id / output_folder)))
                self.send_tcp_worker(response, self.workers[worker]['port'])
                self.workers[worker]['status'] = 'busy' #if len(self.workers) > 1 else 'ready' 
                self.workers[worker]['task'] = self.tasks[0]
                # If there is only one worker, we need to append all tasks in case it dies!
                self.tasks.pop(0)
            elif self.workers[worker]['status'] == 'dead':
                self.tasks.append(self.workers[worker]['task'])
            elif self.workers[worker]['status'] == 'busy':
                logging.info("Worker %s is busy.", worker)
                busy_count += 1
        if busy_count == len(self.workers):
            logging.info("All workers currently busy!")
            return None

    def sort_partition(self, curr_job):
        #get list of input files, ex: "tmp/job-0/mapper-output/file01"
        job_id = 'job-' + str(curr_job['job_id']) + '/'
        tmpPath = Path('tmp/')
        output_direc = Path(tmpPath / job_id / 'mapper-output/')
        map_files = [str(e) for e in output_direc.iterdir() if e.is_file()]
        map_files = sorted(map_files)
        logging.info("MAP FILES: %s", map_files)
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
        #logging.info("TASKS IN SORT_PARTITION: %s", self.tasks)
        self.handle_partition_done = True

    def group_stage(self, curr_job):
        ### Only handles the sorting portion. 
        logging.info("Grouping....")
        for _ in range(len(self.tasks)):
            busy_count = 0
            for worker in self.workers:
                if self.workers[worker]['status'] == 'ready': 
                    job_id = 'job-' + str(curr_job['job_id']) + '/'
                    #tmpPath = Path('tmp/')
                    sort_num = "0" + str(self.sort_dex) if self.sort_dex < 10 else str(self.sort_dex)
                    sort_path = "/sorted" + sort_num
                    #grouper_file_path = Path('tmp'/ job_id / 'grouper-output'/ sort_path)
                    grouper_file_path = 'tmp/' + job_id + 'grouper-output' + sort_path
                    response = {
                        "message_type": "new_sort_task",
                        "input_files": self.tasks[0],
                        "output_file": str(grouper_file_path),
                        "worker_pid": self.workers[worker]['pid']
                    }
                    self.send_tcp_worker(response, self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.tasks[0]
                    self.tasks.pop(0)
                    logging.info("Tasks inside grouping: %s", self.tasks)
                    self.sort_dex += 1
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)
                    busy_count += 1
            if busy_count == len(self.workers):
                logging.info("All workers busy!")
                return None
    
    def prep_reduce(self, curr_job):
        self.handle_partition_done = True
        logging.info("Generating reduce files...")
        # Get all important path info:
        job_id = Path('job-' + str(curr_job['job_id']))
        grouper_folder = Path('tmp' / job_id / 'grouper-output/')
        sorted_files = [str(e) for e in grouper_folder.iterdir() if e.is_file()]
        logging.info("Sorted_Files: %s", sorted_files)
        uniq_key_count = 0
        open_files = []
        file_writers = []
        reduce_files = []
        for f in sorted_files:
            open_files.append(open(f))
        prev_key = None
        for i in range(0, curr_job['num_reducers']):
            f = open(str(grouper_folder) + '/reduce0' + str(i + 1), 'a')
            reduce_files.append(str(grouper_folder) + '/reduce0' + str(i + 1))
            file_writers.append(f)
            
        for line in heapq.merge(*open_files): 
            if prev_key != line.rsplit(" ", 1)[0]:
                uniq_key_count += 1
            prev_key = line.rsplit(" ", 1)[0]
            #check the math for file mapping -> reduce. 
            indx = uniq_key_count % curr_job['num_reducers'] - 1
            file_writers[indx].write(line)
        #Close files. 
        for w in file_writers:
            w.close()
        for f in open_files:
            f.close()

        #self.tasks = sorted([str(e) for e in file_writers])
        #self.tasks = sorted(str(file_writers))
        self.tasks = [reduce_files]
        logging.info("TASKS in PREP-REDUCE: %s", self.tasks)

    def fault_localization(self):
        #UDP SOCKET
        #Determine if a worker is dead, and mark it as 'dead'.
        #How do we determine if a worker is dead? It misses 5 pings or 10 seconds. 
        #We could have an array of timers  
        while True:
            if self.alive is False: break
            for worker in self.workers:
                if self.workers[worker]['status'] == 'ready':
                    try:
                        self.workers[worker]['timer'] -= 1
                    except KeyError:
                        continue
                    if self.workers[worker]['timer'] <= 0:
                        self.workers[worker]['status'] = 'dead'
            time.sleep(1)
        click.echo("Shutting down fault localization...")
 
    def generate_output(self):
        # Copy All Files From Src to Dest:
        job_id = Path('job-' + str(self.curr_job['job_id']))
        reducer_folder = Path('tmp' / job_id / 'reducer-output/')
        for reduce in reducer_folder.glob('*'):
            logging.info("Reduce File: %s", reduce)
            output_dir = Path(self.curr_job['output_directory'] + '/')
            file = str(reduce).split('/')[-1]
            logging.info("File: %s", file)
            output_file = 'outputfile0' + file[-1] if int(file[-1]) < 10 else 'outputfile' + file[-1]
            logging.info("Output_file: %s", output_file)
            try: Path.mkdir(output_dir, parents=True)
            except(FileExistsError):
                pass
            shutil.copyfile(reduce, str(output_dir) + '/' + output_file)
            reduce.unlink()
            logging.info("Moving Complete")


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



"""
        logging.info("Generating reduce files...")
        # Get all important path info:
        job_id = Path('job-' + str(curr_job['job_id']))
        grouper_folder = Path('tmp' / job_id / 'grouper-output/')
        extracted_file = []
        # Loop through all sorted files:
        logging.info("Grouper_Folder: %s", grouper_folder)
        for sorted_file in grouper_folder.glob('sorted*'):
            # Open file
            logging.info("Sorted File: %s", sorted_file)
            with open(str(sorted_file), 'r') as s:
                # Store file contents into list of list of strings
                extracted_file = [line for line in s]
            
            logging.debug("# of Reducers: %s", curr_job['num_reducers'])
            extract_lines = []
            for i in range(0, curr_job['num_reducers']):
                indx = i % curr_job['num_reducers']
                logging.info('Current reduce number: %s', indx + 1)
                #logging.info("Extracted File: %s", extracted_file[0])
                #extract_lines = [extracted_file[line] for line in range(0,len(extracted_file)) if line % curr_job['num_reducers'] == indx]
                for line in range (0,len(extracted_file)):
                    if line % curr_job['num_reducers'] == indx:
                        extract_lines.append(extracted_file[line])
                with open(str(grouper_folder) + "/reduce" + '0' + str(indx + 1), 'a') as f:
                    for words in extract_lines:
                        f.write(words)
                    #logging.debug("Writing for reduce0%s complete.", indx + 1)


"""