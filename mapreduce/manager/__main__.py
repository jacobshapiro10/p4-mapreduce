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
from queue import Queue, Empty

import click


LOGGER = logging.getLogger(__name__)


class Manager:
    """class manager."""

    def __init__(self, host, port):
        """Init everythign."""
        # Core manager state
        self.active_job_indicator = False
        self.shutdown_flag = False
        self.workers = {}
        self.job_id = 0
        # Group per-job state to reduce number of top-level attributes
        # This holds keys like: pending_mt, in_progress, r_maps, partitions,
        # job_dir, mapper_exe, num_reducers, prt, reduce_partitions, ipr,
        # r_reduces, reducer_exe, output_dir
        self.current_job = None
        LOGGER.info("Manager host=%s port=%s pwd=%s", host, port, os.getcwd())
        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            LOGGER.info("Start UDP server thread")
            udp_thread = threading.Thread(target=self.udp_delegate,
                                          args=(host, port))
            udp_thread.daemon = True
            udp_thread.start()
            self.moniter_thread = threading.Thread(target=self.dwm)
            self.moniter_thread.start()
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
            sock.listen()
            sock.settimeout(1)

            job_thread = threading.Thread(target=self.jp, args=(q, tmpdir))
            job_thread.start()

            while not self.shutdown_flag:
                try:
                    clientsocket, _address = sock.accept()
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
                    except OSError:
                        LOGGER.debug("Could not contact worker")
            sys.exit(0)

        if mtype == "new_manager_job":
            q.put(message_dict)
            return

        if mtype == "finished":
            wh = message_dict["worker_host"]
            wp = message_dict["worker_port"]
            key = (wh, wp)

            self.workers[key]["state"] = "ready"

            # Update per-job state if a job is active
            cj = self.current_job
            if cj and key in cj.get("in_progress", {}):
                cj["in_progress"].pop(key, None)
                cj["r_maps"] -= 1
                if cj.get("pending_mt"):
                    self._assign_map_task_to_worker(key)
            elif cj and key in cj.get("ipr", {}):
                cj["ipr"].pop(key, None)
                cj["r_reduces"] -= 1
                if cj.get("prt"):
                    self._assign_reduce_task_to_worker(key)

    def jp(self, q, tmpdir):
        """Process jobs from queue in separate thread."""
        while not self.shutdown_flag:
            try:
                job = q.get(timeout=1)
            except Empty:
                continue
            self.active_job_indicator = True
            self.execute_job(job, tmpdir)
            self.job_id = self.job_id + 1

    def udp_delegate(self, host, port):
        """Listen for UDP heartbeat messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
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
        # Unpack job fields
        input_dir = job["input_directory"]
        output_dir = job["output_directory"]
        nm = job["num_mappers"]

        # Prepare output and job directory
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        job_dir = os.path.join(tmpdir, f"job-{self.job_id:05d}")
        os.makedirs(job_dir, exist_ok=True)

        # Create partitions for mappers
        files = sorted(os.listdir(input_dir))
        partitions = [[] for _ in range(nm)]
        for idx, filename in enumerate(files):
            partitions[idx % nm].append(os.path.join(input_dir, filename))

        # Group per-job state in a single dict to keep instance attributes small
        job_state = {
            "pending_mt": deque(range(nm)),
            "in_progress": {},
            "r_maps": nm,
            "partitions": partitions,
            "job_dir": job_dir,
            "mapper_exe": job["mapper_executable"],
            "num_reducers": job["num_reducers"],
            # reduce-time fields (filled after maps finish)
            "prt": None,
            "reduce_partitions": {},
            "ipr": {},
            "r_reduces": 0,
            "reducer_exe": job["reducer_executable"],
            "output_dir": output_dir,
        }

        self.current_job = job_state

        # Kick off map tasks
        for key, w in self.workers.items():
            if w["state"] == "ready":
                self._assign_map_task_to_worker(key)

        # Wait for maps to finish
        while self.current_job and self.current_job["r_maps"] > 0 and not self.shutdown_flag:
            time.sleep(0.1)

        # Prepare reduce tasks
        rp = self._gather_reduce_partitions(self.current_job["job_dir"],
                                           self.current_job["num_reducers"])

        self.current_job["prt"] = deque(range(self.current_job["num_reducers"]))
        self.current_job["reduce_partitions"] = rp
        self.current_job["ipr"] = {}
        self.current_job["r_reduces"] = self.current_job["num_reducers"]

        # Kick off reduce tasks
        for key, w in self.workers.items():
            if w["state"] == "ready":
                self._assign_reduce_task_to_worker(key)

        # Wait for reduces to finish
        while self.current_job and self.current_job["r_reduces"] > 0 and not self.shutdown_flag:
            time.sleep(0.1)

        # Clean up
        try:
            shutil.rmtree(self.current_job["job_dir"])
        except OSError:
            pass
        self.current_job = None
        self.active_job_indicator = False

    def _assign_reduce_task_to_worker(self, key):
        cj = self.current_job
        if not cj or not cj.get("prt"):
            return

        task_id = cj["prt"].popleft()
        wh, wp = key

        message = {
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "input_paths": cj["reduce_partitions"][task_id],
            "executable": cj["reducer_exe"],
            "output_directory": cj["output_dir"],
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((wh, wp))
                s.sendall(json.dumps(message).encode("utf-8"))
            self.workers[key]["state"] = "busy"
            cj["ipr"][key] = task_id
        except OSError:
            self.workers[key]["state"] = "dead"
            cj["prt"].appendleft(task_id)

    def _assign_map_task_to_worker(self, key):
        cj = self.current_job
        if not cj or not cj.get("pending_mt"):
            return
        task_id = cj["pending_mt"].popleft()
        wh, wp = key

        message = {
            "message_type": "new_map_task",
            "task_id": task_id,
            "input_paths": cj["partitions"][task_id],
            "executable": cj["mapper_exe"],
            "output_directory": cj["job_dir"],
            "num_partitions": cj["num_reducers"],
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((wh, wp))
                s.sendall(json.dumps(message).encode("utf-8"))
            self.workers[key]["state"] = "busy"
            cj["in_progress"][key] = task_id
        except OSError:
            self.workers[key]["state"] = "dead"
            cj["pending_mt"].appendleft(task_id)

    def dwm(self):
        """Detect dead workers and reassign tasks."""
        timeout = 10
        while not self.shutdown_flag:
            now = time.time()
            for key, worker in list(self.workers.items()):
                if self._is_dead(worker, now, timeout):
                    self._mark_dead(worker)
                    self._requeue_map_if_needed(key)
                    self._requeue_reduce_if_needed(key)
            time.sleep(1)

    def _is_dead(self, worker, now, timeout):
        return worker["state"] != "dead" and now - worker["last_hb"] > timeout

    def _mark_dead(self, worker):
        worker["state"] = "dead"

    def _requeue_map_if_needed(self, key):
        cj = self.current_job
        if not cj or key not in cj.get("in_progress", {}):
            return
        task_id = cj["in_progress"].pop(key)
        cj["pending_mt"].appendleft(task_id)
        self._assign_first_ready_worker_to_map()

    def _assign_first_ready_worker_to_map(self):
        for worker_key, w in self.workers.items():
            if w["state"] == "ready" and self.current_job and self.current_job.get("pending_mt"):
                self._assign_map_task_to_worker(worker_key)
                return

    def _requeue_reduce_if_needed(self, key):
        cj = self.current_job
        if not cj or key not in cj.get("ipr", {}):
            return
        task_id = cj["ipr"].pop(key)
        cj["prt"].appendleft(task_id)
        self._assign_first_ready_worker_to_reduce()

    def _assign_first_ready_worker_to_reduce(self):
        for worker_key, w in self.workers.items():
            if w["state"] == "ready" and self.current_job and self.current_job.get("prt"):
                self._assign_reduce_task_to_worker(worker_key)
                return

    def _gather_reduce_partitions(self, job_dir, num_reducers):
        """Return reduce partition mapping for a finished map phase.
        
        for style points.
        """
        job_path = Path(job_dir)
        reduce_parts = {}
        for pid in range(num_reducers):
            pattern = f"maptask*-part{pid:05d}"
            input_paths = sorted(str(p) for p in job_path.glob(pattern))
            reduce_parts[pid] = input_paths
        return reduce_parts


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
        log_handler = logging.FileHandler(logfile)
    else:
        log_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
