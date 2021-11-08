"""See unit test function docstring."""

import json
import time
import distutils.dir_util
import filecmp
import utils
from utils import TESTDATA_DIR
import mapreduce


def worker_message_generator(mock_socket):
    """Fake Worker messages."""
    # Two workers register
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": 1001,
    }).encode('utf-8')
    yield None
    yield json.dumps({
        "message_type": "register",
        "worker_host": "localhost",
        "worker_port": 3002,
        "worker_pid": 1002,
    }).encode('utf-8')
    yield None

    # User submits new job
    yield json.dumps({
        'message_type': 'new_manager_job',
        'input_directory': TESTDATA_DIR/'input',
        'output_directory': 'tmp/test_manager_05/output',
        'mapper_executable': TESTDATA_DIR/'exec/wc_map_slow.sh',
        'reducer_executable': TESTDATA_DIR/'exec/wc_reduce.sh',
        'num_mappers': 2,
        'num_reducers': 1
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for Manager to create directories
    utils.wait_for_isdir("tmp/job-0")

    # Simulate files created by Worker
    distutils.dir_util.copy_tree(
        TESTDATA_DIR/"test_manager_05/intermediate/job-0",
        "tmp/job-0"
    )

    # Wait for Manager to send two map messages because num_mappers=2
    utils.wait_for_map_messages(mock_socket, num=2)

    # Status finished message from one mapper
    yield json.dumps({
        "message_type": "status",
        "output_files": [
            "tmp/job-0/mapper-output/file01",
            "tmp/job-0/mapper-output/file03",
            "tmp/job-0/mapper-output/file05",
            "tmp/job-0/mapper-output/file07",
        ],
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None

    # Wait for Manager to realize that Worker 1002 isn't sending heartbeat
    # messages anymore.  It should then reassign Worker 1002's map task to
    # Worker 1001.
    #
    # We expect a grand total of 3 map messages.  The first two messages are
    # from the Manager assigning two map tasks to two workers.  The third
    # message is from the reassignment.
    #
    # Transfer control back to solution under test in between each check for
    # map messages.
    for _ in utils.wait_for_map_messages_async(mock_socket, num=3):
        yield None

    # Status finished messages from one mapper.  This Worker was reassigned the
    # task that the dead worker failed to complete.
    yield json.dumps({
        "message_type": "status",
        "output_files": [
            "tmp/job-0/mapper-output/file02",
            "tmp/job-0/mapper-output/file04",
            "tmp/job-0/mapper-output/file06",
            "tmp/job-0/mapper-output/file08",
        ],
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None

    # Wait for Manager to send sort job messages
    utils.wait_for_sort_messages(mock_socket)

    # Sort job status finished
    yield json.dumps({
        "message_type": "status",
        "output_file": "tmp/job-0/grouper-output/sorted01",
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None

    # Wait for Manager to send reduce job message
    utils.wait_for_reduce_messages(mock_socket)

    # Reduce job status finished
    yield json.dumps({
        "message_type": "status",
        "output_files": ["tmp/job-0/reducer-output/reduce01"],
        "status": "finished",
        "worker_pid": 1001
    }).encode('utf-8')
    yield None

    # Wait for Output Files to appear
    utils.wait_for_isfile("tmp/test_manager_05/output/outputfile01")

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def worker_heartbeat_generator():
    """Fake heartbeat messages from one good Worker and one dead Worker."""
    # Worker 1002 sends a single heartbeat
    yield json.dumps({
        "message_type": "heartbeat",
        "worker_pid": 1002
    }).encode('utf-8')

    # Worker 1001 continuously sends, but Worker 1002 stops.  This should cause
    # the Manager to detect worker 1002 as dead.
    while True:
        yield json.dumps({
            "message_type": "heartbeat",
            "worker_pid": 1001
        }).encode('utf-8')
        time.sleep(utils.TIME_BETWEEN_HEARTBEATS)


def test_manager_05_dead_worker(mocker):
    """Verify manager handles a dead worker.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_manager_05")

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = worker_message_generator(mock_socket)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.__enter__.return_value.recv.side_effect = \
        worker_heartbeat_generator()

    # Run student manager code.  When student manager calls recv(), it will
    # return the faked responses configured above.
    manager_port, manager_hb_port = utils.get_open_port(nports=2)
    try:
        mapreduce.manager.Manager(manager_port, manager_hb_port)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the manager
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs tests/test_manager_X.py
    messages = utils.get_messages(mock_socket)
    assert messages == [
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_pid": 1001,
            "worker_port": 3001,
        },
        {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_pid": 1002,
            "worker_port": 3002,
        },
        {
            "message_type": "new_worker_task",
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_files": [
                str(TESTDATA_DIR/"input/file01"),
                str(TESTDATA_DIR/"input/file03"),
                str(TESTDATA_DIR/"input/file05"),
                str(TESTDATA_DIR/"input/file07"),
            ],
            "output_directory": "tmp/job-0/mapper-output",
            "worker_pid": 1001,
        },
        {
            "message_type": "new_worker_task",
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_files": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": "tmp/job-0/mapper-output",
            "worker_pid": 1002,
        },
        {
            "message_type": "new_worker_task",
            "executable": str(TESTDATA_DIR/"exec/wc_map_slow.sh"),
            "input_files": [
                str(TESTDATA_DIR/"input/file02"),
                str(TESTDATA_DIR/"input/file04"),
                str(TESTDATA_DIR/"input/file06"),
                str(TESTDATA_DIR/"input/file08"),
            ],
            "output_directory": "tmp/job-0/mapper-output",
            "worker_pid": 1001,
        },
        {
            "message_type": "new_sort_task",
            "input_files": [
                "tmp/job-0/mapper-output/file01",
                "tmp/job-0/mapper-output/file02",
                "tmp/job-0/mapper-output/file03",
                "tmp/job-0/mapper-output/file04",
                "tmp/job-0/mapper-output/file05",
                "tmp/job-0/mapper-output/file06",
                "tmp/job-0/mapper-output/file07",
                "tmp/job-0/mapper-output/file08",
            ],
            "output_file": "tmp/job-0/grouper-output/sorted01",
            "worker_pid": 1001,
        },
        {
            "message_type": "new_worker_task",
            "executable": str(TESTDATA_DIR/"exec/wc_reduce.sh"),
            "input_files": [
                "tmp/job-0/grouper-output/reduce01",
            ],
            "output_directory": "tmp/job-0/reducer-output",
            "worker_pid": 1001,
        },
        {
            "message_type": "shutdown",
        },
    ]

    # Verify grouper output
    assert filecmp.cmp(
        "tmp/job-0/grouper-output/reduce01",
        TESTDATA_DIR/"test_manager_05/correct/job-0/grouper-output/reduce01",
        shallow=False
    )
