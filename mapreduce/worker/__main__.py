"""MapReduce framework Worker node."""
import hashlib
import heapq
import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import threading
from contextlib import ExitStack
import click


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Initialize worker with host/port and start listening threads."""
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown_flag = threading.Event()
        self.heartbeat_thread = None
        """Construct a Worker instance and start listening for messages."""

        LOGGER.info(
            "Worker host=%s port=%s manager_host=%s, manager_port=%s pwd=%s",
            host, port, manager_host, manager_port, os.getcwd())
        listen_thread = threading.Thread(
            target=self.listen_for_messages,
            args=(host, port))
        listen_thread.daemon = True
        listen_thread.start()
        self.register_with_manager(manager_host, manager_port, host, port)
        listen_thread.join()

    def listen_for_messages(self, host, port):
        """Listen for TCP messages sent by the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            LOGGER.info("Start TCP server thread")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            LOGGER.debug("TCP bind %s:%s", host, port)
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown_flag.is_set():
                try:
                    clientsocket, _address = sock.accept()
                except socket.timeout:
                    continue

                with clientsocket:
                    msg = self._recv_json(clientsocket)
                    if not msg:
                        continue
                    LOGGER.debug("TCP recv\n%s", json.dumps(msg, indent=4))

                    msg_type = msg.get("message_type")
                    if msg_type == "register_ack":
                        self._handle_register_ack(host, port)
                    elif msg_type == "shutdown":
                        self._handle_shutdown()
                        break
                    elif msg_type == "new_map_task":
                        self._handle_new_map_task(msg, host, port)
                    elif msg_type == "new_reduce_task":
                        self._handle_new_reduce_task(msg, host, port)

            # Wait for heartbeat thread to finish
            if self.heartbeat_thread is not None:
                self.heartbeat_thread.join()

    def _notify_finished(self, task_id, host, port):
        """Send finished message to Manager."""
        msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": host,
            "worker_port": port,
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.manager_host, self.manager_port))
            s.sendall(json.dumps(msg).encode("utf-8"))

    def _handle_new_map_task(self, msg, host, port):
        """Handle new map task message."""
        task_id = msg["task_id"]
        mapper = msg.get("mapper_executable", msg.get("executable"))
        input_files = msg["input_paths"]
        output_dir = msg["output_directory"]
        num_partitions = msg["num_partitions"]

        LOGGER.info("Starting MapTask(%s) on inputs %s", task_id, input_files)

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmp:
            part_files = [
                open(
                    os.path.join(
                        tmp,
                        f"maptask{task_id:05d}-part{p:05d}"),
                    "w",
                    encoding='utf-8')
                for p in range(num_partitions)
            ]

            for path in input_files:
                with open(path, encoding='utf-8') as infile:
                    with subprocess.Popen(
                        [mapper],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True
                    ) as proc:
                        for line in proc.stdout:
                            key, sep, value = line.partition("\t")
                            if sep:
                                hexdigest = hashlib.md5(
                                    key.encode()).hexdigest()
                                part = int(hexdigest, 16) % num_partitions
                                part_files[part].write(
                                    f"{key}\t{value.rstrip()}\n")

            for f in part_files:
                f.close()

            for p in range(num_partitions):
                fname = f"maptask{task_id:05d}-part{p:05d}"
                fpath = os.path.join(tmp, fname)
                subprocess.run(["sort", "-o", fpath, fpath], check=True)
                shutil.move(fpath, output_dir)

        self._notify_finished(task_id, host, port)
        LOGGER.info("Finished MapTask(%s) and notified Manager.", task_id)

    def _handle_shutdown(self):
        """Handle shutdown message."""
        self.shutdown_flag.set()
        LOGGER.info("Shutdown signal received.")

    def _handle_register_ack(self, host, port):
        """Handle register acknowledgment."""
        LOGGER.debug("Got register_ack RegisterAckMessage()")
        LOGGER.info(
            "Connected to Manager %s:%s",
            self.manager_host,
            self.manager_port)
        self.heartbeat_thread = threading.Thread(
            target=self.send_heartbeats,
            args=(self.manager_host, self.manager_port, host, port),
        )
        LOGGER.info("Start heartbeat thread")
        self.heartbeat_thread.start()

    def _recv_json(self, conn):
        """Receive and parse JSON message from connection."""
        conn.settimeout(1)
        chunks = []
        while True:
            try:
                data = conn.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            chunks.append(data)
        if not chunks:
            return None
        try:
            return json.loads(b"".join(chunks).decode("utf-8"))
        except json.JSONDecodeError:
            return None

    def _handle_new_reduce_task(self, msg, host, port):
        """Handle new reduce task message."""
        task_id = msg["task_id"]
        reducer = msg.get("reducer_executable", msg.get("executable"))
        input_files = msg["input_paths"]
        output_dir = msg["output_directory"]

        LOGGER.info(
            "Starting ReduceTask(%s) with inputs %s",
            task_id,
            input_files)

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmp:
            out_path = os.path.join(tmp, f"part-{task_id:05d}")

            with ExitStack() as stack:
                inputs = [
                    stack.enter_context(open(p, encoding='utf-8'))
                    for p in input_files
                ]
                merged = heapq.merge(*inputs)

                with open(out_path, "w", encoding='utf-8') as out:
                    with subprocess.Popen(
                        [reducer],
                        stdin=subprocess.PIPE,
                        stdout=out,
                        text=True
                    ) as proc:
                        for line in merged:
                            proc.stdin.write(line)

            shutil.move(out_path, output_dir)

        self._notify_finished(task_id, host, port)
        LOGGER.info("Finished ReduceTask(%s) and notified Manager.", task_id)

    def register_with_manager(self, manager_host, manager_port, host, port):
        """Send a registration message to the Manager via TCP."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((manager_host, manager_port))
            except ConnectionRefusedError:
                LOGGER.error(
                    "Could not connect to Manager at %s:%s",
                    manager_host, manager_port
                )
                return

            message_dict = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port,
            }

            sock.sendall(json.dumps(message_dict).encode("utf-8"))
            LOGGER.info(
                "Sent connection request to Manager %s:%s",
                manager_host,
                manager_port
            )

    def send_heartbeats(self, manager_host, manager_port, host, port):
        """Send heartbeat messages to Manager every 2 seconds via UDP."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((manager_host, manager_port))
            sock.settimeout(1)
            while not self.shutdown_flag.is_set():
                message = {
                    "message_type": "heartbeat",
                    "worker_host": host,
                    "worker_port": port,
                }
                try:
                    sock.sendall(json.dumps(message).encode("utf-8"))
                except OSError:
                    continue

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
    formatter = logging.Formatter(
        f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
