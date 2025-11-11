"""MapReduce framework Manager node."""
import json
import logging
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from queue import Queue

import click


LOGGER = logging.getLogger(__name__)


class Manager:
    """class manager."""

    def __init__(self, host, port):
        """Init everythign."""
        self.active_job_indicator = False
        self.shutdown_flag = False
        self.workers = {}
        LOGGER.info("Manager host=%s port=%s pwd=%s", host, port, os.getcwd())
        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            LOGGER.info("Start UDP server thread")
            udp_thread = threading.Thread(target=self.udp_delegate,
                                          args=(host, port))
            udp_thread.daemon = True
            udp_thread.start()
            moniter_thread = threading.Thread(target=self.dwm, daemon=True)
            moniter_thread.start()
            LOGGER.info("Start TCP server thread")
            self.tcp_delegate(host, port, tmpdir)
            udp_thread.join()
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def tcp_delegate(self, host, port, tmpdir):
        """Listen for TCP connections (e.g., Worker registration)."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            q = Queue()
            self.job_id = 0
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            LOGGER.debug(f"TCP bind {host}:{port}")
            sock.listen()
            sock.settimeout(1)

            job_thread = threading.Thread(target=self.jp, args=(q, tmpdir))
            job_thread.start()

            while not self.shutdown_flag:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue

                self._handle_tcp_client(clientsocket, q)

    def _handle_tcp_client(self, clientsocket, q):
        with clientsocket:
            clientsocket.settimeout(1)
            message_chunks = []

            while not self.shutdown_flag:
                try:
                    data = clientsocket.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                message_chunks.append(data)

            if not message_chunks:
                return

            try:
                message_dict = json.loads(
                    b"".join(message_chunks).decode("utf-8")
                    )
            except json.JSONDecodeError:
                return

            LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=4))
            self._process_tcp_message(message_dict, q)

    def _process_tcp_message(self, message_dict, q):
        mtype = message_dict.get("message_type")

        if mtype == "register":
            wh = message_dict["worker_host"]
            wp = message_dict["worker_port"]
            ack = {"message_type": "register_ack"}

            with socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM) as reply_sock:
                try:
                    reply_sock.connect((wh, wp))
                    reply_sock.sendall(json.dumps(ack).encode("utf-8"))
                except OSError:
                    LOGGER.debug("Failed to contact worker")

            LOGGER.info(f"Registered Worker RemoteWorker('{wh}', {wp})")
            self.workers[(wh, wp)] = {"state": "ready", "last_hb": time.time()}
            return

        if mtype == "shutdown":
            self.shutdown_flag = True
            for (wh, wp), worker_info in self.workers.items():
                if worker_info["state"] == "dead":
                    continue
                msg = {"message_type": "shutdown"}
                with socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM) as reply_sock:
                    try:
                        reply_sock.connect((wh, wp))
                        reply_sock.sendall(json.dumps(msg).encode("utf-8"))
                        LOGGER.debug(f"Sent shutdown to {wh}:{wp}")
                    except OSError:
                        LOGGER.debug(f"Could not contact worker {wh}:{wp}")
            sys.exit(0)

        if mtype == "new_manager_job":
            q.put(message_dict)
            return

        if mtype == "finished":
            wh = message_dict["worker_host"]
            wp = message_dict["worker_port"]
            key = (wh, wp)
            task_id = message_dict["task_id"]

            self.workers[key]["state"] = "ready"

            if key in self.in_progress:
                self.in_progress.pop(key, None)
                self.r_maps -= 1
                if self.pending_mt:
                    self._assign_map_task_to_worker(key)
            elif key in self.ipr:
                self.ipr.pop(key, None)
                self.r_reduces -= 1
                if self.prt:
                    self._assign_reduce_task_to_worker(key)

    def jp(self, q, tmpdir):
        """Process jobs from queue in separate thread."""
        while not self.shutdown_flag:
            job = q.get(timeout=1)
            self.active_job_indicator = True
            self.execute_job(job, tmpdir)
            self.job_id = self.job_id + 1

    def udp_delegate(self, host, port):
        """Listen for UDP heartbeat messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            LOGGER.debug(f"UDP bind {host}:{port}")
            sock.settimeout(1)

            while not self.shutdown_flag:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue

                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                if message_dict.get("message_type") == "heartbeat":
                    wh = message_dict["worker_host"]
                    wp = message_dict["worker_port"]
                    key = (wh, wp)
                    if key in self.workers:
                        self.workers[key]["last_hb"] = time.time()

                LOGGER.debug(
                    "UDP recv\n%s", json.dumps(message_dict, indent=4)
                )

    def execute_job(self, job, tmpdir):
        """Execute the job."""
        input_dir = job["input_directory"]
        output_dir = job["output_directory"]
        mapper_exe = job["mapper_executable"]
        reducer_exe = job["reducer_executable"]
        nm = job["num_mappers"]
        num_reducers = job["num_reducers"]
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        job_dir = os.path.join(tmpdir, f"job-{self.job_id:05d}")
        os.makedirs(job_dir, exist_ok=True)
        files = os.listdir(input_dir)
        files = sorted(files)
        partitions = [[] for _ in range(nm)]
        for idx, filename in enumerate(files):
            partitions[idx % nm].append(os.path.join(input_dir, filename))
        self.pending_mt = deque(range(nm))
        self.in_progress = {}
        self.r_maps = nm
        self.partitions = partitions
        self.job_dir = job_dir
        self.mapper_exe = mapper_exe
        self.num_reducers = num_reducers
        for key, w in self.workers.items():
            if w["state"] == "ready":
                self._assign_map_task_to_worker(key)
        while self.r_maps > 0 and not self.shutdown_flag:
            time.sleep(0.1)
        job_path = Path(self.job_dir)
        reduce_tasks = []
        for partition_id in range(self.num_reducers):
            pattern = f"maptask*-part{partition_id:05d}"
            input_paths = sorted(str(p) for p in job_path.glob(pattern))
            reduce_tasks.append((partition_id, input_paths))
        self.prt = deque(range(self.num_reducers))
        self.reduce_partitions = {pid: paths for pid, paths in reduce_tasks}
        self.ipr = {}
        self.r_reduces = self.num_reducers
        self.reducer_exe = reducer_exe
        self.output_dir = output_dir
        for key, w in self.workers.items():
            if w["state"] == "ready":
                self._assign_reduce_task_to_worker(key)
        while self.r_reduces > 0 and not self.shutdown_flag:
            time.sleep(0.1)
        shutil.rmtree(self.job_dir)
        self.active_job_indicator = False

    def _assign_reduce_task_to_worker(self, key):

        if not self.prt:
            return

        task_id = self.prt.popleft()
        wh, wp = key

        message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "input_paths": self.reduce_partitions[task_id],
            "executable": self.reducer_exe,
            "output_directory": self.output_dir,
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((wh, wp))
                s.sendall(json.dumps(message).encode("utf-8"))
            self.workers[key]["state"] = "busy"
            self.ipr[key] = task_id
            LOGGER.info(f"Assigned ReduceTask({task_id}) → Worker({wh},{wp})")
        except OSError:
            self.workers[key]["state"] = "dead"
            self.prt.appendleft(task_id)

    def _assign_map_task_to_worker(self, key):
        if not self.pending_mt:
            return
        task_id = self.pending_mt.popleft()
        wh, wp = key

        message = {
            "message_type": "new_map_task",
            "task_id": task_id,
            "input_paths": self.partitions[task_id],
            "executable": self.mapper_exe,
            "output_directory": self.job_dir,
            "num_partitions": self.num_reducers,
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((wh, wp))
                s.sendall(json.dumps(message).encode("utf-8"))
            self.workers[key]["state"] = "busy"
            self.in_progress[key] = task_id
            LOGGER.info(f"Assigned MapTask({task_id}) → Worker({wh},{wp})")
        except OSError:
            LOGGER.warning(f"Worker {key} unreachable, marking dead.")
            self.workers[key]["state"] = "dead"
            self.pending_mt.appendleft(task_id)

    def dwm(self):
        """dwm."""
        T = 10
        while not self.shutdown_flag:
            n = time.time()
            for key, worker in list(self.workers.items()):
                if worker["state"] != "dead" and n - worker["last_hb"] > T:
                    LOGGER.warning(f"Worker {key} is dead")
                    worker["state"] = "dead"
                    # If this worker was processing a map task, requeue it
                    if key in self.in_progress:
                        task_id = self.in_progress.pop(key)
                        self.pending_mt.appendleft(task_id)
                        LOGGER.info(f"Requeued MapTask({task_id})")
                        # Try to assign to another ready worker
                        for worker_key, w in self.workers.items():
                            if w["state"] == "ready" and self.pending_mt:
                                self._assign_map_task_to_worker(worker_key)
                                break
                    # If this worker was processing a reduce task, requeue it
                    if hasattr(self, 'ipr') and key in self.ipr:
                        task_id = self.ipr.pop(key)
                        self.prt.appendleft(task_id)
                        LOGGER.info(f"Requeued ReduceTask({task_id})")
                        # Try to assign to another ready worker
                        for worker_key, w in self.workers.items():
                            if w["state"] == "ready" and self.prt:
                                self._assign_reduce_task_to_worker(worker_key)
                                break
            time.sleep(1)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
