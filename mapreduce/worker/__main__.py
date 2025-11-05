"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import socket
import threading
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown_flag = threading.Event()  
        """Construct a Worker instance and start listening for messages."""

        LOGGER.info("Worker host=%s port=%s manager_host=%s, manager_port=%s pwd=%s", host, port, manager_host, manager_port, os.getcwd())

        

        listen_thread = threading.Thread(target=self.listen_for_messages, args=(host, port))
        listen_thread.daemon = True
        listen_thread.start()

        self.register_with_manager(manager_host, manager_port, host, port)
        

        # Wait for listener thread to finish (never does, until shutdown)
        listen_thread.join()

  
    def listen_for_messages(self, host, port):
        """Listen for TCP messages sent by the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            LOGGER.info("Start TCP server thread")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            LOGGER.debug(f"TCP bind {host}:{port}")
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown_flag.is_set():
                
                try:
                    
                    clientsocket, address = sock.accept()
                    
                except socket.timeout:
                    
                    continue
                   

                with clientsocket:
                    clientsocket.settimeout(1)
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                    if not message_chunks:
                        continue

                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")

                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue

                    LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=4))
                    if message_dict.get("message_type") == "register_ack":
                        LOGGER.debug("Got register_ack RegisterAckMessage()")   
                        LOGGER.info(f"Connected to Manager {self.manager_host}:{self.manager_port}")    
                        heartbeat_thread = threading.Thread(
                            target=self.send_heartbeats,
                            args=(self.manager_host, self.manager_port, host, port)
                        )
                        LOGGER.info("Start heartbeat thread")

                        heartbeat_thread.daemon = True  # allows clean shutdown
                        heartbeat_thread.start()

                    if message_dict.get("message_type") == "shutdown":
                        self.shutdown_flag.set()
                        LOGGER.info("Shutdown signal received. Exiting Worker.")
                        return
                        
 
    def register_with_manager(self, manager_host, manager_port, host, port):
        """Send a registration message to the Manager via TCP."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((manager_host, manager_port))
            except ConnectionRefusedError:
                LOGGER.error("Could not connect to Manager at %s:%s", manager_host, manager_port)
                return

            message_dict = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port,
            }
            LOGGER.debug(f"TCP send to {manager_host}:{manager_port}\n%s", json.dumps(message_dict, indent=4))

            sock.sendall(json.dumps(message_dict).encode("utf-8"))     
            LOGGER.info(f"Sent connection request to Manager {manager_host}:{manager_port}")


   
    def send_heartbeats(self, manager_host, manager_port, host, port):
        """Send heartbeat messages to Manager every 2 seconds via UDP."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(1)
            while not self.shutdown_flag.is_set():
                message = {
                    "message_type": "heartbeat",
                    "worker_host": host,
                    "worker_port": port,
                }
                try:
                    sock.sendto(json.dumps(message).encode("utf-8"), (manager_host, manager_port))
                except OSError:
                    # Manager might be down — just try again later
                    continue
                
                LOGGER.debug(f"UDP sent to {manager_host}:{manager_port}\n%s", json.dumps(message, indent=4))

                self.shutdown_flag.wait(2)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
