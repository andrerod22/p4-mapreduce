"""See unit test function docstring."""

import socket
import json
import os
import mapreduce
import utils
from utils import TESTDATA_DIR


def test_manager_01_new_job(mocker):
    """Verify manager can receive a new job.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    utils.create_and_clean_testdir("test_manager_02")

    # Mock socket library functions to return sequence of hardcoded values
    # None value terminates while recv loop
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = [
        # New word count job
        json.dumps({
            'message_type': 'new_manager_job',
            'input_directory': TESTDATA_DIR/"input",
            'output_directory': "tmp/test_manager_02/output",
            'mapper_executable': TESTDATA_DIR/'exec/wc_map.sh',
            'reducer_executable': TESTDATA_DIR/'exec/wc_reduce.sh',
            'num_mappers': 2,
            'num_reducers': 1,
        }, cls=utils.PathJSONEncoder).encode('utf-8'),
        None,

        # Shutdown
        json.dumps({
            "message_type": "shutdown",
        }).encode('utf-8'),
        None,
    ]

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket = mocker.patch('socket.socket')
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )
    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.recv.side_effect = \
        utils.worker_heartbeat_generator(1001)

    # Run student manager code.  When student manager calls recv(), it will
    # return the faked responses configured above.
    manager_port, manager_hb_port = utils.get_open_port(nports=2)
    try:
        mapreduce.manager.Manager(manager_port, manager_hb_port)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify directories were created
    assert os.path.isdir("tmp/job-0")
    assert os.path.isdir("tmp/job-0/mapper-output")
    assert os.path.isdir("tmp/job-0/grouper-output")
    assert os.path.isdir("tmp/job-0/reducer-output")

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the manager uses
        # to receive status update JSON messages from the manager.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(('localhost', manager_port)),
        mocker.call().__enter__().listen(),
    ], any_order=True)
