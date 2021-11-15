"""Handle all manager machine operations with multi-threading."""
import os
import logging
import json
import socket
import time
import click
import mapreduce.utils
import re
import heapq
from threading import Thread
from pathlib import Path
import shutil
from json import JSONDecodeError
from collections import defaultdict
# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Manager:
    """Multi-threaded Manager class with 3 main types of threads."""

    def __init__(self, port_number, hb_port_number):
        """Initialize all necessary member variables."""
        logging.info("Starting manager:%s, %s", port_number, hb_port_number)
        logging.info("Manager:%s, %s PWD %s",
                     port_number, hb_port_number, os.getcwd())
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
        self.block_tcp = False
        self.sort_dex = 1
        cwd = Path.cwd()
        tmp_folder = Path(cwd / 'tmp/')
        try:
            Path.mkdir(tmp_folder, parents=True)
        except FileExistsError:
            for job in tmp_folder.glob('job-*'):
                self.remove_jobs(job)
        self.tmp_folder = tmp_folder

        # Create threads:
        logging.debug("Manager:%s, %s", self.port_number, self.hb_port_number)
        udp_thread = Thread(target=self.listen_udp_socket, args=())
        tcp_thread = Thread(target=self.listen_tcp_manager, args=())
        fault_thread = Thread(target=self.fault_localization, args=())
        udp_thread.start()
        tcp_thread.start()
        fault_thread.start()
        udp_thread.join()
        tcp_thread.join()
        fault_thread.join()

    # Thread Specific Functions
    def listen_udp_socket(self):
        """Create UDP Socket to listen for Heartbeats."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.hb_port_number))
            sock.settimeout(1)
            # Receive incoming UDP messages
            while True:
                if self.alive is False:
                    break
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except (JSONDecodeError, TypeError):
                    continue

                try:
                    worker_pid = message_dict['worker_pid']
                    if self.workers[worker_pid]['status'] != 'dead':
                        self.workers[message_dict['worker_pid']]['timer'] = 10
                except KeyError:
                    continue
        logging.debug("Manager:%s Shutting down...", self.port_number)

    def listen_tcp_manager(self):
        """Listen on TCP to messages workers send."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.port_number))
            sock.listen()
            sock.settimeout(1)
            while True:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    for worker in self.workers:
                        if self.workers[worker]['status'] == 'ready':
                            if not self.block_tcp:
                                self.resume_job()
                    continue
                print("Connection from", address[0])
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            # logging.info("didn't get data")
                            break
                        # logging.info("Got Data")
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                logging.debug("Manager:%s received %s",
                              self.port_number, message_dict)
                if message_dict['message_type'] == 'status':
                    if message_dict['status'] == 'finished':
                        pid = message_dict['worker_pid']
                        logging.info("Worker: %s finished.", pid)
                        self.workers[pid]['status'] = 'ready'
                        self.resume_job()
                        logging.info("LEAVING STATUS..")

                response = self.generate_response(message_dict)

                if response['message_type'] == 'register_ack':
                    self.send_tcp_worker(response, response['worker_port'])

                    if self.tasks and not self.block_tcp:
                        self.resume_job()

                elif response['message_type'] == 'shutdown':
                    logging.debug("Shutting down workers: %s", self.workers)
                    for worker in self.workers:
                        if self.workers[worker]['status'] != 'dead':
                            self.send_tcp_worker(response,
                                                 self.workers[worker]['port'])
                    break
                elif response['message_type'] == 'new_manager_job':
                    self.make_job()
                    logging.info("Manager:%s new job number %s",
                                 self.port_number, self.job_ids)
                    self.jobs.append(response)
                    if response == self.jobs[0]:
                        self.stages = ['map', 'group', 'reduce']
                        self.curr_job = self.jobs[0]
                        self.curr_job['job_id'] = self.job_ids
                        self.handle_partioning(self.curr_job['num_mappers'])
                        self.handle_partition_done = True
                        for worker in self.workers:
                            if (self.workers[worker]['status'] == 'ready'
                               and self.jobs and not self.block_tcp):
                                self.execute_job()
                                break
                    else:
                        if not self.block_tcp:
                            self.resume_job()
                    self.job_ids += 1
            self.alive = False
            logging.debug("Manager:%s Shutting down...", self.port_number)

    def make_job(self):
        """Create the directories needed for a job."""
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
        """Remove directories used for a job."""
        path = Path(path)
        for item in path.glob('*'):
            if item.is_file():
                item.unlink()
            else:
                self.remove_jobs(item)
        path.rmdir()

    def execute_job(self):
        """Execute stages in order for a single job."""
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
            logging.info("TASKS LEFT FROM GROUP: %s", self.tasks)
        elif self.stages[0] == 'reduce':
            if not self.handle_partition_done:
                logging.info("Manager:%s end group stage", self.port_number)
                logging.info("Manager:%s begin reduce stage", self.port_number)
                self.prep_reduce(self.curr_job)
            self.mapreduce_stage(self.curr_job, 'reducer')
            if not self.tasks:
                logging.info("Manager:%s end reduce stage", self.port_number)

    def resume_job(self):
        """Resume the stage if the current stage isn't finished."""
        ready_count = 0
        busy_worker = False
        for worker in self.workers:
            if self.workers[worker]['status'] == 'ready':
                ready_count += 1
            elif self.workers[worker]['status'] == 'busy':
                busy_worker = True
        if not self.tasks and not busy_worker:
            # Make sure all workers are ready before moving to next stage
            # Check if any workers, died and reassign tasks:
            alive_count = self.handle_deaths()
            if self.stages and ready_count == alive_count and not self.tasks:
                logging.info("Leaving: %s", self.stages[0])
                self.stages.pop(0)
                self.handle_partition_done = False
                if not self.stages:
                    # Job is done, check queue for next job:
                    # logging.debug("Job is done!")
                    self.generate_output()
                    if self.jobs:
                        prev_job = self.jobs.pop(0)
                        if self.jobs:
                            self.stages = ['map', 'group', 'reduce']
                            self.curr_job = self.jobs[0]
                            self.curr_job['job_id'] = prev_job['job_id'] + 1
                            self.sort_dex = 1
        if self.jobs:
            # logging.debug("Resuming job...")
            self.execute_job()

    def send_tcp_worker(self, response, worker_port):
        """Send a message via TCP to a worker."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect(("localhost", worker_port))
            # send a message
            message = json.dumps(response)
            sock.sendall(message.encode('utf-8'))
            logging.info("Manager:%s sent %s to %s",
                         self.port_number, response, worker_port)

    def generate_response(self, message_dict):
        """Convert worker response to something usable for manager."""
        response = None
        if message_dict['message_type'] == 'shutdown':
            response = {
                "message_type": "shutdown"
            }

        elif message_dict['message_type'] == 'register':
            response = {
                "message_type": "register_ack",
                "worker_host": "localhost",
                "worker_port": message_dict['worker_port'],
                "worker_pid": message_dict['worker_pid']
            }

            self.workers[response['worker_pid']] = {
                'port': response['worker_port'],
                'pid': response['worker_pid'],
                'status': 'ready',
                'task': '',
                'timer': 10
            }

        elif message_dict['message_type'] == 'new_manager_job':
            response = {
                "message_type": "new_manager_job",
                "input_directory": message_dict['input_directory'],
                "output_directory": message_dict['output_directory'],
                "mapper_executable": message_dict['mapper_executable'],
                "reducer_executable": message_dict['reducer_executable'],
                "num_mappers": int(message_dict['num_mappers']),
                "num_reducers": int(message_dict['num_reducers']),
                "full_output_directory":
                "tmp/job-{}/mapper-output".format(self.job_ids)
                if message_dict['output_directory'] == 'output' else None
            }
        elif message_dict['message_type'] == 'status':
            response = {
                'message_type': 'status'
            }
        return response

    def handle_partioning(self, num):
        """Partitions tasks for map and reduce stage."""
        self.handle_partition_done = True
        input_dir = self.curr_job["input_directory"]
        # https://thispointer.com/python-get-list-of-files-in-directory-sorted-by-name/
        input_files = sorted(
                filter(lambda x: os.path.isfile(os.path.join(input_dir, x)),
                       os.listdir(input_dir)))
        input_files = [
            self.curr_job["input_directory"]
            + '/' + file for file in input_files
            ]
        partioned = []
        for i in range(0, num):
            indx = i % num
            tasks = [
                input_files[x] for x in range(len(input_files))
                if x % num == indx
                ]
            partioned.append(tasks)
        self.tasks = partioned

    def mapreduce_stage(self, curr_job, stage):
        """Handle Map and Reduce Stage without distinction."""
        for worker in sorted(self.workers):
            if self.tasks and self.workers[worker]['status'] == 'ready':
                job_id = 'job-' + str(curr_job['job_id']) + '/'
                tmpPath = Path('tmp/')
                output_folder = stage + '-output/'
                response = {
                    "message_type": "new_worker_task",
                    "input_files": self.tasks[0],
                    "executable": curr_job[stage + '_executable'],
                    "output_directory":
                    str(Path(tmpPath / job_id / output_folder)),
                    "worker_pid": self.workers[worker]['pid']
                }
                logging.info("Sending worker task to worker %s",
                             self.workers[worker]['pid'])
                self.send_tcp_worker(response, self.workers[worker]['port'])
                self.workers[worker]['status'] = 'busy'
                self.workers[worker]['task'] = self.tasks[0]
                self.tasks.pop(0)
            elif self.workers[worker]['status'] == 'busy':
                logging.info("Worker %s is busy.", worker)

    def sort_partition(self, curr_job):
        """Handle partition for sorting portion of Group Stage."""
        live_workers = 0
        for worker in self.workers:
            if self.workers[worker]['status'] != 'dead':
                live_workers += 1
        job_id = 'job-' + str(curr_job['job_id']) + '/'
        tmpPath = Path('tmp/')
        output_direc = Path(tmpPath / job_id / 'mapper-output/')
        map_files = [str(e) for e in output_direc.iterdir() if e.is_file()]
        map_files = sorted(map_files)
        logging.info("MAP FILES: %s", map_files)
        partitioned = []
        num_workers = live_workers
        if len(map_files) >= num_workers:
            for i in range(0, num_workers):
                indx = i % num_workers
                tasks = [
                    map_files[x] for x in range(len(map_files))
                    if x % num_workers == indx
                    ]
                partitioned.append(tasks)
        else:  # if number of workers is more than number of files.
            for file in map_files:
                partitioned.append([file])
        self.tasks = partitioned
        self.handle_partition_done = True

    def group_stage(self, curr_job):
        """Handle Group Stage after Map Stage."""
        for _ in range(len(self.tasks)):
            for worker in sorted(self.workers):
                if self.tasks and self.workers[worker]['status'] == 'ready':
                    job_id = 'job-' + str(curr_job['job_id']) + '/'
                    # tmpPath = Path('tmp/')
                    sort_num = (
                        "0" + str(self.sort_dex)
                        if self.sort_dex < 10 else str(self.sort_dex)
                        )
                    sort_path = "/sorted" + sort_num
                    g_path = 'tmp/' + job_id + 'grouper-output' + sort_path
                    response = {
                        "message_type": "new_sort_task",
                        "input_files": self.tasks[0],
                        "output_file": str(g_path),
                        "worker_pid": self.workers[worker]['pid']
                    }
                    self.send_tcp_worker(response,
                                         self.workers[worker]['port'])
                    self.workers[worker]['status'] = 'busy'
                    self.workers[worker]['task'] = self.tasks[0]
                    self.tasks.pop(0)
                    self.sort_dex += 1
                elif self.workers[worker]['status'] == 'busy':
                    logging.info("Worker %s is busy.", worker)

    def prep_reduce(self, curr_job):
        """Create reduce files to get ready for reduce stage."""
        self.handle_partition_done = True
        job_id = Path('job-' + str(curr_job['job_id']))
        grouper_folder = Path('tmp' / job_id / 'grouper-output/')
        sorted_files = [
            str(e) for e in grouper_folder.iterdir() if e.is_file()
            ]
        logging.info("Sorted_Files: %s", sorted_files)
        uniq_key_count = 0
        open_files = []
        file_writers = []
        reduce_files = []
        for f in sorted_files:
            open_files.append(open(f))
        prev_key = None
        for i in range(0, curr_job['num_reducers']):
            file_num = (
                "0" + str(i + 1) if (i + 1) < 10
                else str(i + 1)
            )
            f = open(str(grouper_folder) + '/reduce' + file_num, 'a')
            reduce_files.append(str(grouper_folder) + '/reduce0' + str(i + 1))
            file_writers.append(f)

        for line in heapq.merge(*open_files):
            if prev_key != line.rsplit(" ", 1)[0]:
                uniq_key_count += 1
            prev_key = line.rsplit(" ", 1)[0]
            # check the math for file mapping -> reduce.
            indx = uniq_key_count % curr_job['num_reducers'] - 1
            file_writers[indx].write(line)
        # Close files.
        for w in file_writers:
            w.close()
        for f in open_files:
            f.close()
        self.tasks = [reduce_files]

    def fault_localization(self):
        """Fault thread used to count down timer for workers and more."""
        keyErr = False
        while True:
            if not self.alive:
                break
            for worker in self.workers:
                try:
                    if self.workers[worker]['status'] != 'dead':
                        self.workers[worker]['timer'] -= 1
                    if (self.workers[worker]['timer'] <= 0
                       and self.workers[worker]['status'] != 'dead'):
                        self.workers[worker]['status'] = 'dead'
                        self.block_tcp = True
                        self.resume_job()
                        self.block_tcp = False
                except KeyError:
                    keyErr = True
                    break
            if not keyErr:
                time.sleep(1)
            keyErr = False
        # click.echo("Shutting down fault localization...")

    def generate_output(self):
        """Generate the output files at the end of a job."""
        job_id = Path('job-' + str(self.curr_job['job_id']))
        reducer_folder = Path('tmp' / job_id / 'reducer-output/')
        for reduce in reducer_folder.glob('*'):
            output_dir = Path(self.curr_job['output_directory'] + '/')
            file = str(reduce).split('/')[-1]
            # logging.info("File: %s", file)
            splitNum = re.split('(\\d+)', file)
            # output_file = 'outputfile0' + file[-1] if
            # int(file[-1]) < 10 else 'outputfile' + file[-1]
            number = -1
            for num in splitNum:
                if num.isdigit():
                    number = int(num)
                    break
            # logging.info("ENDNUM: %s ", number)
            output_file = ('outputfile0' + str(number)
                           if number < 10 else 'outputfile' + str(number))
            logging.debug("Output_file: %s", output_file)
            try:
                Path.mkdir(output_dir, parents=True)
            except FileExistsError:
                pass
            shutil.copyfile(reduce, str(output_dir) + '/' + output_file)
            reduce.unlink()

    def handle_deaths(self):
        """Redistribute tasks assigned to a dead worker."""
        alive = 0
        for worker in self.workers:
            if self.workers[worker]['status'] == 'dead':
                if self.workers[worker]['task']:
                    self.tasks.append(self.workers[worker]['task'])
                    self.workers[worker]['task'] = None
            else:
                alive += 1
        return alive


@click.command()
@click.argument("port_number", nargs=1, type=int)
@click.argument("hb_port_number", nargs=1, type=int)
def main(port_number, hb_port_number):
    """Initialize Constructor and begin work."""
    Manager(port_number, hb_port_number)
    # Main Manager Thread Ending
    click.echo("Shutting down manager...")


if __name__ == '__main__':
    main()
