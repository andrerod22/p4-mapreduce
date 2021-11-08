"""See unit test function docstring."""

import socket
import json
import mapreduce
import utils


def test_manager_00_shutdown(mocker):
    """Verify manager shutdowns down.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    # Mock socket library functions to return sequence of hardcoded values
    # None value terminates while recv loop
    mock_socket = mocker.patch('socket.socket')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = [
        json.dumps({"message_type": "shutdown"}).encode('utf-8'),
        None
    ]

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.recv.side_effect = \
        utils.worker_heartbeat_generator(1001)

    # Run student manager code.  When student manager calls recv(), it will
    # receive the faked responses configured above.  When the student code
    # calls sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    # The manager may have several threads, so we make sure that they have all
    # been stopped.

    manager_port, manager_hb_port = utils.get_open_port(nports=2)
    try:
        mapreduce.manager.Manager(manager_port, manager_hb_port)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify that the student code called the correct socket functions with
    # the correct arguments.
    #
    # NOTE: to see a list of all calls
    # >>> print(mock_socket.mock_calls)
    mock_socket.assert_has_calls([
        # TCP socket server configuration.  This is the socket the manager uses
        # to receive JSON formatted commands from mapreduce-submit.
        mocker.call(socket.AF_INET, socket.SOCK_STREAM),
        mocker.call().__enter__().setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1,
        ),
        mocker.call().__enter__().bind(('localhost', manager_port)),
        mocker.call().__enter__().listen(),
    ], any_order=True)


def test_manager_00_shutdown_workers(mocker):
    """Verify manager shuts down and tells workers to shut down.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.
    """
    # Mock socket library functions to return sequence of hardcoded values
    # None value terminates while recv loop
    mock_socket = mocker.patch('socket.socket')
    mock_oskill = mocker.patch('os.kill')
    mockclientsocket = mocker.MagicMock()
    mockclientsocket.recv.side_effect = [
        # First fake worker registers with manager
        json.dumps({
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3001,
            "worker_pid": 1001,
        }).encode('utf-8'),
        None,
        # Second fake worker registers with manager
        json.dumps({
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 3002,
            "worker_pid": 1002,
        }).encode('utf-8'),
        None,
        # Fake shutdown message sent to manager
        json.dumps({
            "message_type": "shutdown",
        }).encode('utf-8'),
        None,
    ]

    # Mock accept function returns mock client socket and (address, port) tuple
    mock_socket.return_value.__enter__.return_value.accept.return_value = (
        mockclientsocket,
        ("127.0.0.1", 10000),
    )

    # Mock socket library functions to return heartbeat messages
    mock_socket.return_value.__enter__.return_value.recv.side_effect = \
        utils.worker_heartbeat_generator(1001)

    # Run student manager code.  When student manager calls recv(), it will
    # return the faked responses configured above.
    manager_port, manager_hb_port = utils.get_open_port(nports=2)
    try:
        mapreduce.manager.Manager(manager_port, manager_hb_port)
        utils.wait_for_threads()
    except SystemExit as error:
        assert error.code == 0

    # Verify student code did not call os.kill(), which is prohibited.
    # Instead, solutions should send a TCP message to the worker, telling it to
    # shut down.
    assert not mock_oskill.called, "os.kill() is prohibited"

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

        # Manager should have sent two shutdown messages, one to each worker
        mocker.call().__enter__().sendall(json.dumps({
            "message_type": "shutdown",
        }).encode('utf-8')),
        mocker.call().__enter__().sendall(json.dumps({
            "message_type": "shutdown",
        }).encode('utf-8')),
    ], any_order=True)
