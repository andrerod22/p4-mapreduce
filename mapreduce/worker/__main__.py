import os
import logging
import json
import time
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, manager_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
            "worker_pid": 77811
        }
        logging.debug(
            "Worker:%s received\n%s",
            worker_port,
            json.dumps(message_dict, indent=2),
        )

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")
        time.sleep(120)


@click.command()
@click.argument("manager_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_port, worker_port):
    Worker(manager_port, worker_port)


if __name__ == '__main__':
    main()
