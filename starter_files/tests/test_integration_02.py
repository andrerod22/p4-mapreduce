"""See unit test function docstring."""

import multiprocessing
from pathlib import Path
import pytest
import mapreduce
import utils
from utils import TESTDATA_DIR


@pytest.fixture(name="processes")
def processes_setup_teardown():
    """Store list of Manager and Worker processes, then clean up after test.

    The code before the "yield" statement is setup code, which is executed
    before the test.  Code after the "yield" is teardown code, which is
    executed at the end of the test.  Teardown code is executed whether the
    test passed or failed.
    """
    # Setup code: store a list of processes.  Later, the testcase will append
    # to this list.
    processes = []

    # Transfer control to testcase
    yield processes

    # Teardown code: force all processes to terminate
    for process in processes:
        process.terminate()
        process.join()


def test_integration_02_wordcount(processes):
    """Run a word count MapReduce job."""
    utils.create_and_clean_testdir("test_integration_02")

    # Start Manager
    manager_port, manager_hb_port, *worker_ports = \
        utils.get_open_port(nports=4)
    process = multiprocessing.Process(
        name=f"Manager:{manager_port}",
        target=mapreduce.Manager,
        args=(manager_port, manager_hb_port)
    )
    process.start()
    processes.append(process)
    utils.wait_for_process_is_ready(process, port=manager_port)

    # Start workers
    for worker_port in worker_ports:
        process = multiprocessing.Process(
            name=f"Worker:{worker_port}",
            target=mapreduce.Worker,
            args=(manager_port, manager_hb_port, worker_port),
        )
        process.start()
        processes.append(process)
        utils.wait_for_process_is_ready(process, port=worker_port)

    # Send new manager job message
    utils.send_message({
        "message_type": "new_manager_job",
        "input_directory": TESTDATA_DIR/"input",
        "output_directory": "tmp/test_integration_02/output",
        "mapper_executable": TESTDATA_DIR/"exec/wc_map.sh",
        "reducer_executable": TESTDATA_DIR/"exec/wc_reduce.sh",
        "num_mappers": 2,
        "num_reducers": 1
    }, port=manager_port)

    # Wait for manager to create output
    utils.wait_for_isfile("tmp/test_integration_02/output/outputfile01")

    outfile01 = Path("tmp/test_integration_02/output/outputfile01")
    word_count_correct = Path(TESTDATA_DIR/"correct/word_count_correct.txt")
    # Verify final output file contents
    with outfile01.open(encoding='utf-8') as infile:
        actual = sorted(infile.readlines())
    with word_count_correct.open(encoding='utf-8') as infile:
        correct = sorted(infile.readlines())
    assert actual == correct

    # Send shutdown message
    utils.send_message({
        "message_type": "shutdown"
    }, port=manager_port)

    # Wait for processes to stop
    utils.wait_for_process_all_stopped(processes)

    # Check for clean exit
    for process in processes:
        assert process.exitcode == 0, \
            f"{process.name} exitcode={process.exitcode}"
