import os
import logging
import json
import time
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Manager:
    def __init__(self, tcp_port, udp_port):
        logging.info("Starting manager:%s, %s", tcp_port, udp_port)
        logging.info("Manager:%s, %s PWD %s", tcp_port, udp_port, os.getcwd())

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port1": 6001,
            "worker_port2": 6002,
            "worker_pid1": 77811,
            "worker_pid2": 77893,
        }
        logging.debug("Manager:%s, %s received\n%s",
            tcp_port, udp_port, 
            json.dumps(message_dict, indent=2),
        )

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        logging.debug("IMPLEMENT ME!")
        time.sleep(15)


@click.command()
@click.argument("tcp_port", nargs=1, type=int)
@click.argument("udp_port", nargs=1, type=int)
def main(tcp_port, udp_port):
    Manager(tcp_port, udp_port)
    print("Shutting down manager...")


if __name__ == '__main__':
    main()
