"""See unit test function docstring."""

import json
import socket
import mapreduce
import utils


def test_worker_00_shutdown(mocker):
    """Verify worker shuts down.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    # get open ports before patch is done
    manager_port, manager_hb_port, worker_port = utils.get_open_port(nports=3)

    # Mock socket library functions to return sequence of hardcoded values
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = [
        # Fake a message sent by the manager.  When the student worker
        # calls recv(), it will receive a shutdown message.
        json.dumps({"message_type": "shutdown"}).encode('utf-8'),

        # None value terminates while recv loop
        None,
    ]

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
            worker_port  # Worker port
        )
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the worker uses
        # to receive status update JSON messages from the manager.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(('localhost', worker_port)),
        mocker.call().__enter__().listen(),
    ], any_order=True)
