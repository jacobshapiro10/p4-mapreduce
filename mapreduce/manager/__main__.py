"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import click
import socket
import threading
import mapreduce.utils
import sys
from queue import Queue
import shutil
import time


LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        self.active_job_indicator = False
        self.shutdown_flag = False


        self.workers = {}
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info("Manager host=%s port=%s pwd=%s", host, port, os.getcwd())

        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

            LOGGER.info("Start UDP server thread")
            udp_thread = threading.Thread(target=self.udp_delegate, args=(host, port))
            udp_thread.daemon = True
            udp_thread.start()

            LOGGER.info("Start TCP server thread")
            self.tcp_delegate(host, port, tmpdir)

            moniter_thread = threading.Thread(target=self.dead_worker_monitor, daemon=True)
            moniter_thread.start()
            

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

            # Start job processor thread
            job_thread = threading.Thread(target=self.job_processor, args=(q, tmpdir))
            job_thread.start()

            while not self.shutdown_flag:

                
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue

                
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
                        continue

                    message_bytes = b"".join(message_chunks)
                    message_str = message_bytes.decode("utf-8")

                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue

                
                    LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=4))


                    if message_dict.get("message_type") == "register":
                        worker_host = message_dict["worker_host"]
                        worker_port = message_dict["worker_port"]

                        ack = {
                            "message_type": "register_ack",
                        }

                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reply_sock:
                            try:
                                reply_sock.connect((worker_host, worker_port))
                                reply_sock.sendall(json.dumps(ack).encode("utf-8"))
                                LOGGER.debug(f"TCP send to {worker_host}:{worker_port}\n%s", json.dumps(ack, indent=4))
                            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                                LOGGER.debug("Hi")

                        LOGGER.info(f"Registered Worker RemoteWorker('{worker_host}', {worker_port})")
                        self.workers[(worker_host, worker_port)] = {"state": "ready", "last_hb": time.time()}


                    elif message_dict.get("message_type") == "shutdown":
                        self.shutdown_flag = True
                        for worker_host, worker_port in self.workers:
                            shutdown_msg = {"message_type": "shutdown"}
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reply_sock:
                                try:
                                    reply_sock.connect((worker_host, worker_port))
                                    reply_sock.sendall(json.dumps(shutdown_msg).encode("utf-8"))
                                    LOGGER.debug(f"Sent shutdown to {worker_host}:{worker_port}")
                                except OSError:
                                    LOGGER.debug(f"Could not contact worker {worker_host}:{worker_port}")

                        # Exit tcp_delegate → causes Manager to end
                        sys.exit(0)


                    elif message_dict.get("message_type") == "new_manager_job":
                        q.put(message_dict)
                        # Don't process here - let job_processor thread handle it

                        
                                            

    def job_processor(self, q, tmpdir):
        """Process jobs from queue in separate thread."""
        while not self.shutdown_flag:
            try:
                job = q.get(timeout=1)
            except:
                continue
            
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
                if message_str.get("message_type") == "heartbeat":
                    wh = message_dict["worker_host"]
                    wp = message_dict["worker_port"]
                    key = (wh, wp)
                    if key in self.workers:
                        self.workers[key]["last_hb"] = time.time()

                
                LOGGER.debug("UDP recv\n%s", json.dumps(message_dict, indent=4))



    def execute_job(self, job, tmpdir):
        input_dir = job["input_directory"]
        output_dir = job["output_directory"]
        mapper_exe = job["mapper_executable"]
        reducer_exe = job["reducer_executable"]
        num_mappers = job["num_mappers"]
        num_reducers = job["num_reducers"]


        if os.path.exists(job["output_directory"]):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        job_dir = os.path.join(tmpdir, f"job-{self.job_id:05d}")
        os.makedirs(job_dir, exist_ok=True)



        files = os.listdir(input_dir)
        files = sorted(files)


        partitions = [[] for _ in range(num_mappers)]

    
        for idx, filename in enumerate(files):
            partitions[idx % num_mappers].append(os.path.join(input_dir, filename))

        



        from collections import deque

        self.pending_map_tasks = deque(range(num_mappers))   
        self.in_progress = {}                               
        self.remaining_maps = num_mappers
        self.partitions = partitions
        self.job_dir = job_dir
        self.mapper_exe = mapper_exe
        self.num_reducers = num_reducers

        for key, w in self.workers.items():
            if w["state"] == "ready":
                self._assign_map_task_to_worker(key)





        #handle rest of code

        self.active_job_indicator = False





    def _assign_map_task_to_worker(self, key):
        
        if not self.pending_map_tasks:
            return
        task_id = self.pending_map_tasks.popleft()
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
            self.pending_map_tasks.appendleft(task_id)


    def dead_worker_monitor(self):
        import time
        TIMEOUT = 6
        while not self.shutdown_flag:
            now = time.time()
            for key, worker in list(self.workers.items()):
                if worker["state"] != "dead" and now - worker["last_hb"] > TIMEOUT:
                    LOGGER.warning(f"Worker {key} is dead")
                    worker["state"] = "dead"
                    # If this worker was processing a map task, requeue it:
                    if key in self.in_progress:
                        task_id = self.in_progress.pop(key)
                        self.pending_map_tasks.appendleft(task_id)
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