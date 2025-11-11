"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import socket
import threading
import mapreduce.utils
import hashlib
import shutil
import tempfile
import subprocess
import heapq
from contextlib import ExitStack


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, m_host, m_port):
        self.m_host = m_host
        self.m_port = m_port
        self.shutdown_flag = threading.Event()
        """Construct a Worker instance and start listening for messages."""

        LOGGER.info
        ("Worker host=%s port=%s manager_host=%s, manager_port=%s pwd=%s",
         host, port, m_host, m_port, os.getcwd())
        listen_thread = threading.Thread(target=self.listen_for_messages,
                                         args=(host, port))
        listen_thread.daemon = True
        listen_thread.start()
        self.register_with_manager(m_host, m_port, host, port)
        listen_thread.join()

    def listen_for_messages(self, host, port):
        """Listen for TCP messages sent by the Manager."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        LOGGER.info("Start TCP server thread")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        LOGGER.debug(f"TCP bind {host}:{port}")
        sock.listen()
        sock.settimeout(1)

        while not self.shutdown_flag.is_set():
            try:
                clientsocket, _ = sock.accept()
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

        sock.close()

    def _notify_finished(self, task_id, host, port):
        msg = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": host,
            "worker_port": port,
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.m_host, self.m_port))
            s.sendall(json.dumps(msg).encode("utf-8"))
        LOGGER.info(f"Finished task {task_id} and notified Manager.")

    def _handle_new_map_task(self, msg, host, port):
        task_id = msg["task_id"]
        m_exe = msg["executable"]
        output_dir = msg["output_directory"]
        n = msg["num_partitions"]
        input_paths = msg["input_paths"]

        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
                ) as tmp:
            part_files = [open(os.path.join(
                tmp, f"maptask{task_id:05d}-part{p:05d}"), "w"
                )
                        for p in range(n)]
            for path in input_paths:
                with open(path) as infile, subprocess.Popen(
                    [m_exe], stdin=infile, stdout=subprocess.PIPE, text=True
                ) as proc:
                    for line in proc.stdout:
                        k, sep, v = line.partition("\t")
                        if not sep:
                            continue
                        v = v.rstrip("\n")
                        part = int(hashlib.md5(k.encode()).hexdigest(), 16) % n
                        part_files[part].write(f"{k}\t{v}\n")

            for f in part_files:
                f.close()
            for p in range(n):
                fn = os.path.join(tmp, f"maptask{task_id:05d}-part{p:05d}")
                subprocess.run(["sort", "-o", fn, fn], check=True)
                shutil.move(fn, output_dir)

        self._notify_finished(task_id, host, port)

    def _handle_shutdown(self):
        self.shutdown_flag.set()
        if hasattr(self, "hb_t") and self.hb_t.is_alive():
            self.hb_t.join(timeout=1)
        LOGGER.info("Shutdown signal received.")

    def _handle_register_ack(self, host, port):
        LOGGER.debug("Got register_ack RegisterAckMessage()")
        LOGGER.info(f"Connected to Manager {self.m_host}:{self.m_port}")
        self.hb_t = threading.Thread(
            target=self.send_heartbeats,
            args=(self.m_host, self.m_port, host, port),
            daemon=True,
        )
        LOGGER.info("Start heartbeat thread")
        self.hb_t.start()

    def _recv_json(self, conn):
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
        task_id = msg["task_id"]
        reducer_exe = msg["executable"]
        input_paths = msg["input_paths"]
        output_dir = msg["output_directory"]

        with tempfile.TemporaryDirectory(prefix=f"mr-t{task_id:05d}-") as tmp:
            with ExitStack() as stack:
                files = [stack.enter_context(open(p)) for p in input_paths]
                merged = heapq.merge(*files)

                out_file = os.path.join(tmp, f"part-{task_id:05d}")
                with open(out_file, "w") as out, subprocess.Popen(
                    [reducer_exe], text=True, stdin=subprocess.PIPE, stdout=out
                ) as proc:
                    for line in merged:
                        proc.stdin.write(line)

            shutil.move(out_file, output_dir)

        self._notify_finished(task_id, host, port)

    def register_with_manager(self, m_host, m_port, host, port):
        """Send a registration message to the Manager via TCP."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((m_host, m_port))
            except ConnectionRefusedError:
                LOGGER.error(
                    "Could not connect to Manager at %s:%s",
                    m_host, m_port
                    )
                return

            message_dict = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port,
            }
            LOGGER.debug(f"TCP send to {m_host}:{m_port}\n%s",
                         json.dumps(message_dict, indent=4))

            sock.sendall(json.dumps(message_dict).encode("utf-8"))
            LOGGER.info(
                f"Sent connection request to Manager {m_host}:{m_port}"
            )

    def send_heartbeats(self, m_host, m_port, host, port):
        """Send heartbeat messages to Manager every 2 seconds via UDP."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((m_host, m_port))
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
                    # Manager might be down — just try again later
                    continue
                LOGGER.debug(
                    f"UDP sent to {m_host}:{m_port}\n%s",
                    json.dumps(message, indent=4)
                    )

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
