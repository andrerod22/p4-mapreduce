"""Unit test utilities used by many tests."""
from contextlib import ExitStack
import os
import subprocess
import pathlib
import shutil
import json
import time
import threading
import socket


# Temporary directory.  Tests will create files here.
TMPDIR = pathlib.Path("tmp")

# Directory containing unit test input files, mapper executables,
# reducer executables, etc.
TESTDATA_DIR = pathlib.Path(__file__).parent/"testdata"

# Time in between two heart beats.  This in the spec.
TIME_BETWEEN_HEARTBEATS = 2

# Set default timeout and long timeout for tests where we need to
# wait for workers to die.
#
# We'll need longer wait times on slow machines like the autograder.
if pathlib.Path("/home/autograder/working_dir").exists():
    TIMEOUT = 30
    TIMEOUT_LONG = 60
else:
    TIMEOUT = 10
    TIMEOUT_LONG = 30


class PathJSONEncoder(json.JSONEncoder):
    """
    Extended the Python JSON encoder to encode Pathlib objects.

    Docs: https://docs.python.org/3/library/json.html

    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """

    # Avoid pylint false positive.  There's a style problem in the JSON library
    # that we're inheriting from.
    # https://github.com/PyCQA/pylint/issues/414#issuecomment-212158760
    # pylint: disable=E0202

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)


def create_and_clean_testdir(basename):
    """Remove and recreate {TMPDIR}/basename then remove {TMPDIR}/job-*."""
    # Clean up {TMPDIR}/job-*/
    for jobdir in TMPDIR.glob("job-*"):
        shutil.rmtree(jobdir)

    # Clean up {TMDIR}/basename/
    dirpath = TMPDIR/basename
    if dirpath.exists():
        shutil.rmtree(dirpath)
    dirpath.mkdir(parents=True)


def worker_heartbeat_generator(*worker_pids):
    """Fake Worker heartbeat messages."""
    while True:
        time.sleep(TIME_BETWEEN_HEARTBEATS)
        # Avoid sending heartbeats too fast
        for pid in worker_pids:
            time.sleep(TIME_BETWEEN_HEARTBEATS)
            # Avoid sending heartbeats too fast
            yield json.dumps({
                "message_type": "heartbeat",
                "worker_pid": pid
            }).encode('utf-8')


def get_messages(mock_socket):
    """Return a list decoded JSON messages sent via mock_socket sendall()."""
    messages = []

    # Add sendall returns that do and do not use context managers
    args_list = \
        mock_socket.return_value.__enter__.return_value.sendall.call_args_list

    for args, _ in args_list:
        message_str = args[0].decode('utf-8')
        message_dict = json.loads(message_str)
        messages.append(message_dict)

    return messages


def is_register_message(message):
    """Return True if message is worker registration."""
    return (
        "message_type" in message and
        message["message_type"] == "register"
    )


def is_map_message(message):
    """Return True if message starts a map job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_worker_task" and

        "output_directory" in message and
        "mapper-output" in message["output_directory"]
    )


def is_sort_message(message):
    """Return True if message is a sort job message."""
    return (
        "message_type" in message and
        message["message_type"] == "new_sort_task"
    )


def is_reduce_message(message):
    """Return True if message starts a reduce job."""
    return (
        "message_type" in message and
        message["message_type"] == "new_worker_task" and

        "output_directory" in message and
        "reducer-output" in message["output_directory"]
    )


def is_status_finished_message(message):
    """Return True message is a status finished message."""
    return (
        "message_type" in message and
        message["message_type"] == "status" and
        "status" in message and
        message["status"] == "finished"
    )


def is_heartbeat_message(message):
    """Return True if message is a heartbeat message."""
    return (
        "message_type" in message and
        message["message_type"] == "heartbeat"
    )


def filter_heartbeat_messages(messages):
    """Return a subset of messages including only heartbeat messages."""
    return [m for m in messages if is_heartbeat_message(m)]


def filter_not_heartbeat_messages(messages):
    """Return a subset of messages excluding heartbeat messages."""
    return [m for m in messages if not is_heartbeat_message(m)]


def wait_for_isdir(*args):
    """Verify manager created the correct job directory structure."""
    for _ in range(TIMEOUT):
        if all(os.path.isdir(dirname) for dirname in args):
            return
        time.sleep(1)
    raise Exception(f"Failed to create directories {args}")


def wait_for_isfile(*args):
    """Verify manager created the correct job directory structure."""
    for _ in range(TIMEOUT):
        if all(os.path.exists(filename) for filename in args):
            return
        time.sleep(1)
    raise Exception(f"Failed to create files {args}")


def wait_for_messages(function, mock_socket, num=1):
    """Return when function() evaluates to True on num messages."""
    for _ in wait_for_messages_async(function, mock_socket, num):
        pass


def wait_for_messages_async(function, mock_socket, num=1):
    """Yield every 1s, return when function()==True on num messages."""
    for _ in range(TIMEOUT_LONG):
        messages = get_messages(mock_socket)
        n_true_messages = sum(function(m) for m in messages)
        if n_true_messages == num:
            return
        yield
        time.sleep(1)
    raise Exception(f"Expected {num} messages, got {n_true_messages}.")


def wait_for_status_finished_messages(mock_socket, num=1):
    """Return after num status finished messages."""
    return wait_for_messages(is_status_finished_message, mock_socket, num)


def wait_for_register_messages(mock_socket, num=1):
    """Return after num register messages."""
    return wait_for_messages(is_register_message, mock_socket, num)


def wait_for_map_messages(mock_socket, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_map_message, mock_socket, num)


def wait_for_map_messages_async(mock_socket, num=1):
    """Return after num map messages, yield between iterations."""
    return wait_for_messages_async(is_map_message, mock_socket, num)


def wait_for_reduce_messages(mock_socket, num=1):
    """Return after num map messages."""
    return wait_for_messages(is_reduce_message, mock_socket, num)


def wait_for_reduce_messages_async(mock_socket, num=1):
    """Return after num map messages, yield between iterations.."""
    return wait_for_messages_async(is_reduce_message, mock_socket, num)


def wait_for_sort_messages(mock_socket, num=1):
    """Return after num sort messages."""
    return wait_for_messages(is_sort_message, mock_socket, num)


def wait_for_sort_messages_async(mock_socket, num=1):
    """Return after num sort messages, yield between iterations.."""
    return wait_for_messages_async(is_sort_message, mock_socket, num)


def wait_for_log(caplog, match_str):
    """Check for match_str in manager logging msgs."""
    for _ in range(TIMEOUT):
        if match_str in caplog.text:
            return True
        time.sleep(1)

    raise Exception(f"Did not find '{match_str}' in logs")


def wait_for_threads(num=1):
    """Return after the total number of threads is num."""
    for _ in range(TIMEOUT):
        if len(threading.enumerate()) == num:
            return
        time.sleep(1)
    raise Exception("Failed to close threads.")


def send_message(message, port):
    """Send JSON-encoded TCP message."""
    host = "localhost"
    message_str = json.dumps(message, cls=PathJSONEncoder)
    message_bytes = str.encode(message_str)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.sendall(message_bytes)


def port_is_open(host, port):
    """Return True if host:port is open."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
        except ConnectionRefusedError:
            return False
        else:
            return True


def wait_for_process_is_ready(process, port):
    """Return when process is listening on port."""
    for _ in range(TIMEOUT):
        assert process.exitcode is None, \
            f"{process.name} unexpected exit with exitcode={process.exitcode}"
        if port_is_open("localhost", port):
            return
        time.sleep(1)
    raise Exception(
        f"Process {process.name} PID {process.pid} failed to start \
        listening on port {port}.")


def wait_for_process_all_stopped(processes):
    """Return after all processes are not alive."""
    for _ in range(TIMEOUT):
        if all(not p.is_alive() for p in processes):
            return
        time.sleep(1)
    raise Exception("Some processes failed to stop.")


def assert_no_prohibited_terms(*terms):
    """Check for prohibited terms before testing style."""
    for term in terms:
        completed_process = subprocess.run(
            [
                "grep",
                "-r",
                "-n",
                term,
                "--include=*.py",
                "--exclude=submit.py",
                "mapreduce"
            ],
            check=False,  # We'll check the return code manually
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

        # Grep exit code should be non-zero, indicating that the prohibited
        # term was not found.  If the exit code is zero, crash and print a
        # helpful error message with a filename and line number.
        assert completed_process.returncode != 0, (
            f"The term '{term}' is prohibited.\n{completed_process.stdout}"
        )


def get_open_port(nports=1):
    """Return a port or list of ports available for use on localhost."""
    ports = []
    with ExitStack() as stack:
        for _ in range(nports):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            stack.enter_context(sock)
            sock.bind(('', 0))
            port = sock.getsockname()[1]
            ports.append(port)
    return ports if len(ports) > 1 else ports[0]
