import argparse
import csv
import json
import logging
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pymongo import MongoClient

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
conf_template = """
storage:
  dbPath: /home/ubuntu/mongodb/{data_scale}/data/
  #dbPath: /home/ubuntu/mongodb/small/data/
  #dbPath: /home/ubuntu/mongodb/medium/data/
  #dbPath: /home/ubuntu/mongodb/large/data/
  journal:
    enabled: true

systemLog:
  quiet: false
  logRotate: reopen
  destination: file
  logAppend: true
  path: /var/log/mymongod.log

replication:
  replSetName: rs0
  enableMajorityReadConcern: true

net:
  port: {port}
  bindIp: {addr}
"""
mymongod_service_template = """
[Unit]
Description=Aviatrix MongoDb Daemon
ConditionFileIsExecutable=/usr/bin/mongod
After=network.target
[Service]
ExecStart=/usr/bin/mongod --config /etc/mymongod.conf
Restart=always
LimitNOFILE=64000
[Install]
WantedBy=multi-user.target
"""

class Test:
    def __init__(self, test_name: str, data_scale: str, max_concurrency: int, output_directory_path: str, clear_cache: bool, db_addr: str, db_port: int):
        self._ts = int(time.time())
        self._test_name = test_name
        self._data_scale = data_scale
        self._max_concurrency = max_concurrency
        self._clear_cache = clear_cache
        self._db_addr = db_addr
        self._db_port = db_port
        self._dpath = Path(output_directory_path)
        self._tbl_headers = ("parallel reads", "avg_time (ms)", "total_duration (ms)", "start_duration (ms)", "end_duration (ms)", "stdev")
        self._join_pipeline = [
            # {"$match": {"_id": "alta-gcp-euw1-s4~-~qaaas-lab24"}},
            {"$lookup": {"from": "gateways", "localField": "_id", "foreignField": "vpc_id", "as": "gw_data"}},
            # {"$limit": 2000}
        ]
        _aws_vendor_names = ["AWS", "AWS GOV", "AWS CHINA", "AWS Top Secret", "AWS Secret"]
        self._aws_gw_filter = {"vendor_name": { "$in": _aws_vendor_names}}
        self._aws_vpc_filter = {"vpc_data.vendor_name": { "$in": _aws_vendor_names}}
        if data_scale.lower() == "large":
            self._target_gw_name, self._target_vpc_id = "aws-noninsane-spoke2-11-hagw", "vpc-0e0b773666399bbde"
        elif data_scale.lower() == "medium":
            self._target_gw_name, self._target_vpc_id = "alta-azure-usw2-s50-hagw", "0a30f7f0-92ee-40c1-a8cc-ffcf8218fa17"
        elif data_scale.lower() == "small":
            self._target_gw_name, self._target_vpc_id = "overlap-sol-transit-hagw", "1bcbce6d-1839-491f-a432-7508119f3648"
        self.stop_all_unrelated_services()
        p = Path("/etc/mymongod.conf")
        p.write_text(conf_template.format(data_scale=data_scale, port=db_port, addr=db_addr))
        subprocess.check_call("systemctl restart mymongod.service", shell=True)

    def _run_aggregate_query(self, client: MongoClient | None):
        gw_count = 0
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        ts_start = time.time_ns()
        for doc in client.diansan.vpcs.aggregate(self._join_pipeline):
            gw_count += len(doc["gw_data"])
        ts_end = time.time_ns()
        return (ts_start, ts_end, gw_count)

    def _join_gw_vpc(self, concurrency: int, multiprocess: bool):
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            client = MongoClient(self._db_addr, self._db_port)
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_aggregate_query, client) for _ in range(concurrency)]
        return [f.result() for f in futures]


    def with_threadpool_join(self, concurrency: int):
        return self._join_gw_vpc(concurrency, False)

    def with_procpool_join(self, concurrency: int):
        return self._join_gw_vpc(concurrency, True)


    def _run_find_one_query(self, collname: str, query: dict, client: MongoClient | None):
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        ts_start = time.time_ns()
        obj = client.diansan[collname].find(query)[0]
        obj_len = len(obj.items())
        ts_end = time.time_ns()
        return (ts_start, ts_end, obj_len)

    def _find_gw(self, concurrency: int, gw_name: str | None, multiprocess: bool):
        client = MongoClient(self._db_addr, self._db_port)
        if gw_name is None:
            gw_name = random.choice(list(client.diansan.gateways.find({}, {"gw_name": 1, "_id": 0})))["gw_name"]
        logger.warning(f"GW Name: {gw_name}")
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_one_query, "gateways", {"gw_name": gw_name}, client) for _ in range(concurrency)]
        return [f.result() for f in futures]
    def with_threadpool_find_gw(self, concurrency: int):
        return self._find_gw(concurrency, self._target_gw_name, False)
    def with_threadpool_find_rand_gw(self, concurrency: int):
        return self._find_gw(concurrency, None, False)
    def with_procpool_find_gw(self, concurrency: int):
        return self._find_gw(concurrency, self._target_gw_name, True)
    def with_procpool_find_rand_gw(self, concurrency: int):
        return self._find_gw(concurrency, None, True)

    def _find_vpc(self, concurrency: int, vpc_id: str | None, multiprocess: bool):
        client = MongoClient(self._db_addr, self._db_port)
        if vpc_id is None:
            vpc_id = random.choice(list(client.diansan.vpcs.find({}, {"_id": 1})))["_id"]
        logger.warning(f"VPC doc ID: {vpc_id}")
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_one_query, "vpcs", {"_id": vpc_id}, client) for _ in range(concurrency)]
        return [f.result() for f in futures]
    def with_threadpool_find_vpc(self, concurrency: int):
        return self._find_vpc(concurrency, self._target_vpc_id, False)
    def with_threadpool_find_rand_vpc(self, concurrency: int):
        return self._find_vpc(concurrency, None, False)
    def with_procpool_find_vpc(self, concurrency: int):
        return self._find_vpc(concurrency, self._target_vpc_id, True)
    def with_procpool_find_rand_vpc(self, concurrency: int):
        return self._find_vpc(concurrency, None, True)


    def _run_find_many_query(self, collname, query, client):
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        ts_start = time.time_ns()
        obj_count = 0
        for obj in client.diansan[collname].find(query):
            _ = list(obj.items())
            obj_count += 1
        ts_end = time.time_ns()
        return (ts_start, ts_end, obj_count)

    def _list_docs(self, concurrency: int, multiprocess: bool, coll_name: str, find_filter: dict):
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            client = MongoClient(self._db_addr, self._db_port)
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_many_query, coll_name, find_filter, client) for _ in range(concurrency)]
        return [f.result() for f in futures]

    def with_threadpool_list_aws_gws(self, concurrency: int):
        return self._list_docs(concurrency, False, "gateways", self._aws_gw_filter)

    def with_procpool_list_aws_gws(self, concurrency: int):
        return self._list_docs(concurrency, True, "gateways", self._aws_gw_filter)

    def with_threadpool_list_aws_vpcs(self, concurrency: int):
        return self._list_docs(concurrency, False, "vpcs", self._aws_vpc_filter)

    def with_procpool_list_aws_vpcs(self, concurrency: int):
        return self._list_docs(concurrency, True, "vpcs", self._aws_vpc_filter)



    def with_threadpool_list_all_gws(self, concurrency: int):
        return self._list_docs(concurrency, False, "gateways", {})

    def with_procpool_list_all_gws(self, concurrency: int):
        return self._list_docs(concurrency, True, "gateways", {})

    def with_threadpool_list_all_vpcs(self, concurrency: int):
        return self._list_docs(concurrency, False, "vpcs", {})

    def with_procpool_list_all_vpcs(self, concurrency: int):
        return self._list_docs(concurrency, True, "vpcs", {})


    def _get_test_funcs(self):
        return (
            self.with_procpool_find_gw,
            self.with_procpool_find_vpc,
            self.with_threadpool_find_gw,
            self.with_threadpool_find_vpc,
            self.with_threadpool_list_aws_gws,
            self.with_procpool_list_aws_gws,
            self.with_threadpool_list_aws_vpcs,
            self.with_procpool_list_aws_vpcs,
            self.with_procpool_join,
            self.with_threadpool_join,
            self.with_procpool_find_rand_gw,
            self.with_procpool_find_rand_vpc,
            self.with_threadpool_find_rand_gw,
            self.with_threadpool_find_rand_vpc,
            self.with_threadpool_list_all_gws,
            self.with_procpool_list_all_gws,
            self.with_threadpool_list_all_vpcs,
            self.with_procpool_list_all_vpcs,
        )

    @staticmethod
    def get_avg_stdev(res):
        ts_diffs_in_ms = [(t[1] - t[0]) // 1_000_000 for t in res]
        avg = statistics.fmean(ts_diffs_in_ms)
        stdev = statistics.pstdev(ts_diffs_in_ms)
        return avg, stdev
    @staticmethod
    def get_start_duration(res):
        return (max(t[0] for t in res) - min(t[0] for t in res)) // 1_000_000
    @staticmethod
    def get_end_duration(res):
        return (max(t[1] for t in res) - min(t[1] for t in res)) // 1_000_000
    @staticmethod
    def get_total_duration(res):
        return (max(t[1] for t in res) - min(t[0] for t in res)) // 1_000_000
    @staticmethod
    def show(res, details=False):
        total_duration = Test.get_total_duration(res)
        avg, stdev = Test.get_avg_stdev(res)
        logger.warning(f"{total_duration=}, {avg=}, {stdev=}")
        if details:
            for i in res:
                logger.warning(i)

    def stop_all_unrelated_services(self):
        procs_to_stop = "perfmon below rrdcached assetd etcd mongod cloudxd apache2 avx-autoscale spire-controller spire-gateway avx-ctrl-state-sync apache-spiffe-helper avx-ctrl-appserver"
        subprocess.check_call(f"systemctl stop {procs_to_stop}", shell=True)

    # def restore_from_backup(self, backup_path: str):
    #     self.stop_all_unrelated_services()
    #     p = Path("/lib/systemd/system/mymongod.service")
    #     p.write_text(mymongod_service_template)
    #     subprocess.check_call(f"systemctl daemon-reload", shell=True)

    def test(self) -> dict:
        res = {}
        _client = MongoClient(self._db_addr, self._db_port)
        for f in self._get_test_funcs():
            res[f] = {}
        self._dpath.mkdir(mode=0o755, parents=True, exist_ok=True)
        # subprocess.check_call(f"rm -rf {self._dpath.as_posix()}", shell=True)
        for func in res.keys():
            temp = [self._tbl_headers]
            for concurrency in range(1, self._max_concurrency + 1):
                if self._clear_cache:
                    subprocess.check_call("systemctl restart mymongod.service", shell=True)
                    while True:
                        try:
                            _ = _client.diansan.gateways.find({}).limit(1)[0].keys()
                            break
                        except Exception as e:
                            logger.error(e)
                            time.sleep(1)
                logger.warning(f"starting {func.__name__} - {concurrency}")
                t = func(concurrency)
                logger.warning(f"ended {func.__name__} - {concurrency}")
                avg, stdev = Test.get_avg_stdev(t)
                temp.append((concurrency, avg, Test.get_total_duration(t), Test.get_start_duration(t), Test.get_end_duration(t), stdev))
                res[func][concurrency] = t

            fname = "_".join((self._test_name, self._data_scale, str(self._ts), func.__name__)) + ".csv"
            fpath = self._dpath.joinpath(fname).as_posix()
            with open(fpath, "w", newline="", encoding="utf-8") as file:
                wr = csv.writer(file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
                wr.writerows(temp)
            # with open(self._dpath.joinpath(func.__name__+".txt").as_posix(), "w", encoding="utf-8") as file:
            #     json.dump(res[func], file, ensure_ascii=False, indent=4)
            #     logger.warning(file.read())
        return res


if __name__ == "__main__":
    # python3 mongodb_test.py -n test1 -s small -c 3 -d /tmp/result -a localhost -p 8964
    # t = Test('test1', "small", 2, "/tmp/result/", False, "localhost", 8964)
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--test_name", action="store", type=str, required=True)
    parser.add_argument("-s", "--data_scale", action="store", choices=["large", "small"], type=bool, required=True)
    parser.add_argument("-c", "--concurrency", action="store", type=int, default=os.cpu_count() * 2)
    parser.add_argument("-d", "--output_directory_path", action="store", type=str, default="/tmp/result")
    parser.add_argument("-cc", "--clear_cache", action="store_true", type=bool, default=False)
    parser.add_argument("-a", "--db_server_addr", action="store", type=str, default="localhost")
    parser.add_argument("-p", "--db_server_port", action="store", type=int, default=8964)
    _args = parser.parse_args(sys.argv[1:])
    t = Test(_args.test_name, _args.data_scale, _args.concurrency, _args.output_directory_path, _args.clear_cache, _args.db_server_addr, _args.db_server_port)
    res = t.test()
