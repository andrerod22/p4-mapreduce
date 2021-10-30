import os
import logging
import json
import time
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    def __init__(self, tcp_port, udp_port, worker_port):
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": worker_port,
            "worker_pid": os.getpid()
        }
        logging.debug(
            "Worker:%s received\n%s",
            worker_port,
            json.dumps(message_dict, indent=2),
        )

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")
        time.sleep(15)


@click.command()
@click.argument("tcp_port", nargs=1, type=int)
@click.argument("udp_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(tcp_port, udp_port, worker_port):
    Worker(tcp_port, udp_port, worker_port)
    print("shutting down worker...")

if __name__ == '__main__':
    main()
