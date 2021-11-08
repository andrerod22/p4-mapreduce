"""See unit test function docstring."""

import os
import json
from pathlib import Path
import utils
import mapreduce
from utils import TESTDATA_DIR


def manager_message_generator(mock_socket):
    """Fake Manager messages."""
    # Worker Register
    utils.wait_for_register_messages(mock_socket)
    yield json.dumps({
        "message_type": "register_ack",
        "worker_host": "localhost",
        "worker_port": 3001,
        "worker_pid": os.getpid(),
    }).encode('utf-8')
    yield None

    # Map job
    yield json.dumps({
        'message_type': 'new_worker_task',
        'executable': TESTDATA_DIR/'exec/wc_map.sh',
        'input_files': [
            TESTDATA_DIR/'input/file01',
            TESTDATA_DIR/'input/file02',
        ],
        'output_directory': "tmp/test_worker_07",
        'worker_pid': os.getpid(),
    }, cls=utils.PathJSONEncoder).encode('utf-8')
    yield None

    # Wait for worker to finish map job
    utils.wait_for_status_finished_messages(mock_socket)

    # Group job
    yield json.dumps({
        'message_type': 'new_sort_task',
        'input_files': [
            'tmp/test_worker_07/file01',
            'tmp/test_worker_07/file02',
        ],
        'output_file': "tmp/test_worker_07/group.out",
        'worker_pid': os.getpid()
    }).encode('utf-8')
    yield None

    # Wait for worker to finish sort job.  There should now be two
    # status=finished messages in total.  One from the previous map task, and
    # one from this sort task.
    utils.wait_for_status_finished_messages(mock_socket, num=2)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode('utf-8')
    yield None


def test_worker_07_two_inputs(mocker):
    """Verify worker correctly completes a job with two input files.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_worker_07")

    # get open ports before patch is done
    manager_port, manager_hb_port, worker_port = utils.get_open_port(nports=3)

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = manager_message_generator(mock_socket)

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Run student worker code.  When student worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            manager_port,  # Manager port
            manager_hb_port,  # Manager heartbeat port
            worker_port,  # Worker port
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker
    all_messages = utils.get_messages(mock_socket)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": os.getpid(),
        },
        {
            "message_type": "status",
            "output_files": [
                "tmp/test_worker_07/file01",
                "tmp/test_worker_07/file02"
            ],
            "status": "finished",
            "worker_pid": os.getpid(),
        },
        {
            "message_type": "status",
            "output_file": "tmp/test_worker_07/group.out",
            "status": "finished",
            "worker_pid": os.getpid(),
        },
    ]

    # Verify map stage output
    with Path("tmp/test_worker_07/file01").open(encoding='utf-8') as infile:
        mapout01 = infile.readlines()
    with Path("tmp/test_worker_07/file02").open(encoding='utf-8') as infile:
        mapout02 = infile.readlines()
    assert sorted(mapout01 + mapout02) == [
        "\t1\n",
        "\t1\n",
        "bye\t1\n",
        "goodbye\t1\n",
        "hadoop\t1\n",
        "hadoop\t1\n",
        "hello\t1\n",
        "hello\t1\n",
        "world\t1\n",
        "world\t1\n",
    ]

    # Verify group stage output
    with Path("tmp/test_worker_07/group.out").open(encoding='utf-8') as infile:
        groupout = infile.readlines()
    assert groupout == [
        "\t1\n",
        "\t1\n",
        "bye\t1\n",
        "goodbye\t1\n",
        "hadoop\t1\n",
        "hadoop\t1\n",
        "hello\t1\n",
        "hello\t1\n",
        "world\t1\n",
        "world\t1\n",
    ]
