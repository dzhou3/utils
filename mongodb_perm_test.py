import argparse
import csv
import json
import logging
import os
from pathlib import Path
import random
import subprocess
import sys
import time
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from bson import ObjectId
from pymongo import MongoClient
from pymongo import ASCENDING

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


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class QueryPerfResult:
    def __init__(self, q_start_ts: int, q_end_ts: int, total_size: int, total_count: int):
        self.q_start_ts = q_start_ts
        self.q_end_ts = q_end_ts
        self.total_size = total_size
        self.total_count = total_count

class Row:
    def __init__(self, parallel_read: int, data: List[QueryPerfResult]):
        self._parallel_read = parallel_read
        self._data = data
    def to_tuple(self, scale_time: int = 1_000_000) -> Tuple[int, float, float, float, float, int, int, int, int, float, int]:
        # default: convert time unit ns to ms
        min_size, max_size = Row.get_min_max_size(self._data)
        min_count, max_count = Row.get_min_max_count(self._data)
        max_diff_size = abs(max_size - min_size)
        max_diff_size_in_kb = max_diff_size // 1024
        avg_size = Row.get_avg_size(self._data)
        avg_size_in_kb = avg_size // 1024
        return (
            self._parallel_read, 
            Row.get_avg_time(self._data) / scale_time,
            Row.get_total_duration(self._data) / scale_time,
            Row.get_start_duration(self._data) / scale_time,
            Row.get_end_duration(self._data) / scale_time,
            avg_size,
            avg_size_in_kb,
            max_diff_size,
            max_diff_size_in_kb,
            Row.get_avg_count(self._data),
            max_count - min_count,
        )
    @staticmethod
    def get_headers() -> Tuple[str, str, str, str, str, str, str, str, str]:
        return ("parallel reads", "avg_time (ms)", "total_duration (ms)", "start_duration (ms)", "end_duration (ms)", "avg_size", "avg_size_kb", "max_diff_size", "max_diff_size_kb", "avg_count", "max_diff_count")
    @staticmethod
    def get_avg_time(data: List[QueryPerfResult]) -> float:
        return sum((qpr.q_end_ts - qpr.q_start_ts) for qpr in data) / len(data)
    @staticmethod
    def get_min_max_start_ts(data: List[QueryPerfResult]) -> Tuple[int, int]:
        return min(qpr.q_start_ts for qpr in data), max(qpr.q_start_ts for qpr in data)
    @staticmethod
    def get_start_duration(data: List[QueryPerfResult]) -> int:
        _min, _max = Row.get_min_max_start_ts(data)
        return _max - _min
    @staticmethod
    def get_min_max_end_ts(data: List[QueryPerfResult]) -> Tuple[int, int]:
        return min(qpr.q_end_ts for qpr in data), max(qpr.q_end_ts for qpr in data)
    @staticmethod
    def get_end_duration(data: List[QueryPerfResult]) -> int:
        _min, _max = Row.get_min_max_end_ts(data)
        return _max - _min
    @staticmethod
    def get_total_duration(data: List[QueryPerfResult]) -> int:
        start_min, _ = Row.get_min_max_start_ts(data)
        _, end_max = Row.get_min_max_end_ts(data)
        return end_max - start_min
    @staticmethod
    def get_min_max_count(data: List[QueryPerfResult]) -> Tuple[int, int]:
        return min(qpr.total_count for qpr in data), max(qpr.total_count for qpr in data)
    @staticmethod
    def get_avg_count(data: List[QueryPerfResult]) -> float:
        return sum(qpr.total_count for qpr in data) / len(data)
    @staticmethod
    def get_min_max_size(data: List[QueryPerfResult]) -> Tuple[int, int]:
        return min(qpr.total_size for qpr in data), max(qpr.total_size for qpr in data)
    @staticmethod
    def get_avg_size(data: List[QueryPerfResult]) -> int:
        return sum(qpr.total_size for qpr in data) // sum(qpr.total_count for qpr in data)

class Result:
    def __init__(self, table_size: int, dpath: Path, name: str, data_scale: str, timestamp: int, test_name: str):
        self.dpath = dpath
        self.name = name
        self.data_scale = data_scale
        self.ts = timestamp
        self.test_name = test_name
        self.data = [None] * table_size
    
    def add(self, parallel_read: int, qpr_list: List[QueryPerfResult]) -> None:
        self.data[parallel_read - 1] = Row(parallel_read, qpr_list)

    def to_csv(self):
        table = [Row.get_headers()]
        for row in self.data:
            table.append(row.to_tuple())
        fname = "_".join((self.name, self.data_scale, str(self.ts), self.test_name)) + ".csv"
        fpath = self.dpath.joinpath(fname).as_posix()
        with open(fpath, "w", newline="", encoding="utf-8") as file:
            wr = csv.writer(file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
            wr.writerows(table)

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
        self._join_pipeline = {"$lookup": {"from": "gateways", "localField": "_id", "foreignField": "vpc_id", "as": "gw_data"}}
        _aws_vendor_names = ["AWS", "AWS GOV", "AWS CHINA", "AWS Top Secret", "AWS Secret"]
        self._aws_gw_filter = {"vendor_name": { "$in": _aws_vendor_names}}
        self._aws_vpc_filter = {"vpc_data.vendor_name": { "$in": _aws_vendor_names}}
        self.stop_all_unrelated_services()

        p = Path("/etc/mymongod.conf")
        p.write_text(conf_template.format(data_scale=data_scale, port=db_port, addr=db_addr))
        subprocess.check_call("systemctl restart mymongod.service", shell=True)

        # if data_scale.lower() == "large":
        #     self._target_gw_name, self._target_vpc_id = "aws-noninsane-spoke2-11-hagw", "vpc-0e0b773666399bbde"
        # elif data_scale.lower() == "medium":
        #     self._target_gw_name, self._target_vpc_id = "alta-azure-usw2-s50-hagw", "0a30f7f0-92ee-40c1-a8cc-ffcf8218fa17"
        # elif data_scale.lower() == "small":
        #     self._target_gw_name, self._target_vpc_id = "overlap-sol-transit-hagw", "1bcbce6d-1839-491f-a432-7508119f3648"
        c = MongoClient(db_addr, db_port, maxPoolSize=1)
        self._target_gw_name = list(c.diansan.gateways.find({}, {"gw_name": 1, "_id": 0}).limit(1).sort([("$natural", -1)]))[0]["gw_name"]
        self._target_vpc_id = list(c.diansan.vpcs.find({}, {"_id": 1}).limit(1).sort([("$natural", -1)]))[0]["_id"]
        self._total_vpc_docs = c.diansan.vpcs.count_documents({})
        self._total_gw_docs = c.diansan.gateways.count_documents({})
        self._all_gw_names = list(set(doc["gw_name"] for doc in c.diansan.gateways.find({}, {"gw_name": 1, "_id": 0})))
        self._all_vpc_ids = list(set(doc["_id"] for doc in c.diansan.vpcs.find({}, {"_id": 1})))
        c.close()
        self._coll_2_count = {"gateways": self._total_gw_docs, "vpcs": self._total_vpc_docs}
        logger.info(f"{self._target_gw_name=}, {self._target_vpc_id=}, {self._total_vpc_docs=}, {self._total_gw_docs=}")

    def _run_aggregate_query(self, sort_by: List[Tuple[str, int]], page_size: int, page_num: int | None, client: MongoClient | None) -> QueryPerfResult:
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        if page_num is None:
            # avoid the last page as it may have less than page_size docs
            page_num = random.randint(1, max(1, self._total_vpc_docs // page_size))
        sort_stage = {"$sort": {key: order for key, order in sort_by}}
        pipeline = [sort_stage, {"$skip": page_size * (page_num - 1)}, {"$limit": page_size}, self._join_pipeline]
        total_count = total_size = 0
        ts_start = time.time_ns()
        for doc in client.diansan.vpcs.aggregate(pipeline):
            total_size += len(json.dumps(doc, cls=JSONEncoder).encode('utf-8'))
            total_count += 1
        ts_end = time.time_ns()
        return QueryPerfResult(ts_start, ts_end, total_size, total_count)

    def _join_gw_vpc(self, concurrency: int, multiprocess: bool, sort_by: List[Tuple[str, int]], page_size: int, page_num: int | None) -> List[QueryPerfResult]:
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            client = MongoClient(self._db_addr, self._db_port)
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_aggregate_query, sort_by, page_size, page_num, client) for _ in range(concurrency)]
        return [f.result() for f in futures]

    def with_threadpool_join(self, concurrency: int, page_num: int = 1) -> List[QueryPerfResult]:
        return self._join_gw_vpc(concurrency, False, [("_id", ASCENDING)], self._coll_2_count["vpcs"], page_num)

    def with_procpool_join(self, concurrency: int, page_num: int = 1) -> List[QueryPerfResult]:
        return self._join_gw_vpc(concurrency, True, [("_id", ASCENDING)], self._coll_2_count["vpcs"], page_num)

    def with_threadpool_join_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        return self._join_gw_vpc(concurrency, False, [("_id", ASCENDING)], page_size, None)

    def with_procpool_join_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        return self._join_gw_vpc(concurrency, True, [("_id", ASCENDING)], page_size, None)

    def with_threadpool_join_2nd_last_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        # avoid the last page as it may have less than page_size docs
        page_num = max(1, self._total_vpc_docs // page_size)
        return self._join_gw_vpc(concurrency, False, [("_id", ASCENDING)], page_size, page_num)

    def with_procpool_join_2nd_last_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        # avoid the last page as it may have less than page_size docs
        page_num = max(1, self._total_vpc_docs // page_size)
        return self._join_gw_vpc(concurrency, True, [("_id", ASCENDING)], page_size, page_num)


    def _run_find_one_query(self, collname: str, query: Dict, client: MongoClient | None) -> QueryPerfResult:
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        total_count = total_size = 0
        ts_start = time.time_ns()
        for doc in client.diansan[collname].find(query):
            total_size += len(json.dumps(doc, cls=JSONEncoder).encode('utf-8'))
            total_count += 1
        ts_end = time.time_ns()
        return QueryPerfResult(ts_start, ts_end, total_size, total_count)

    def _find_gw(self, concurrency: int, gw_name: str, multiprocess: bool) -> List[QueryPerfResult]:
        client = MongoClient(self._db_addr, self._db_port)
        logger.warning(f"GW Name: {gw_name}")
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_one_query, "gateways", {"gw_name": gw_name}, client) for _ in range(concurrency)]
        return [f.result() for f in futures]
    def with_threadpool_find_gw(self, concurrency: int) -> List[QueryPerfResult]:
        return self._find_gw(concurrency, self._target_gw_name, False)
    def with_threadpool_find_rand_gw(self, concurrency: int) -> List[QueryPerfResult]:
        gw_name = random.choice(self._all_gw_names)
        return self._find_gw(concurrency, gw_name, False)
    def with_procpool_find_gw(self, concurrency: int) -> List[QueryPerfResult]:
        return self._find_gw(concurrency, self._target_gw_name, True)
    def with_procpool_find_rand_gw(self, concurrency: int) -> List[QueryPerfResult]:
        gw_name = random.choice(self._all_gw_names)
        return self._find_gw(concurrency, gw_name, True)
    def _find_vpc(self, concurrency: int, vpc_id: str, multiprocess: bool) -> List[QueryPerfResult]:
        client = MongoClient(self._db_addr, self._db_port)
        logger.warning(f"VPC doc ID: {vpc_id}")
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_one_query, "vpcs", {"_id": vpc_id}, client) for _ in range(concurrency)]
        return [f.result() for f in futures]
    def with_threadpool_find_vpc(self, concurrency: int) -> List[QueryPerfResult]:
        return self._find_vpc(concurrency, self._target_vpc_id, False)
    def with_threadpool_find_rand_vpc(self, concurrency: int) -> List[QueryPerfResult]:
        vpc_id = random.choice(self._all_vpc_ids)
        return self._find_vpc(concurrency, vpc_id, False)
    def with_procpool_find_vpc(self, concurrency: int) -> List[QueryPerfResult]:
        return self._find_vpc(concurrency, self._target_vpc_id, True)
    def with_procpool_find_rand_vpc(self, concurrency: int) -> List[QueryPerfResult]:
        vpc_id = random.choice(self._all_vpc_ids)
        return self._find_vpc(concurrency, vpc_id, True)


    def _run_find_many_query(self, collname: str, query: Dict, sort_by: List[Tuple[str, int]], page_size: int, page_num: int | None, client: MongoClient | None) -> QueryPerfResult:
        if client is None:
            client = MongoClient(self._db_addr, self._db_port)
        obj_count = 0
        if page_num is None:
            page_num = random.randint(1, max(1, self._coll_2_count[collname] // page_size))
        docs_skipped = page_size * (page_num - 1)
        total_count = total_size = 0
        ts_start = time.time_ns()
        for doc in client.diansan[collname].find(query).sort(sort_by).skip(docs_skipped).limit(page_size):
            total_size += len(json.dumps(doc, cls=JSONEncoder).encode('utf-8'))
            total_count += 1
        ts_end = time.time_ns()
        return QueryPerfResult(ts_start, ts_end, total_size, total_count)

    def _list_docs(self, concurrency: int, multiprocess: bool, coll_name: str, find_filter: Dict, sort_by: List[Tuple[str, int]], page_size: int, page_num: int | None) -> List[QueryPerfResult]:
        if multiprocess:
            client = None
            executor = ProcessPoolExecutor
        else:
            client = MongoClient(self._db_addr, self._db_port)
            executor = ThreadPoolExecutor
        with executor(max_workers=concurrency) as exe:
            futures = [exe.submit(self._run_find_many_query, coll_name, find_filter, sort_by, page_size, page_num, client) for _ in range(concurrency)]
        return [f.result() for f in futures]

    def with_threadpool_list_aws_gws(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "gateways"
        return self._list_docs(concurrency, False, coll_name, self._aws_gw_filter, [("gw_name", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_procpool_list_aws_gws(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "gateways"
        return self._list_docs(concurrency, True, coll_name, self._aws_gw_filter, [("gw_name", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_threadpool_list_aws_vpcs(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, False, coll_name, self._aws_vpc_filter, [("_id", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_procpool_list_aws_vpcs(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, True, coll_name, self._aws_vpc_filter, [("_id", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_threadpool_list_all_gws(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "gateways"
        return self._list_docs(concurrency, False, coll_name, {}, [("gw_name", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_procpool_list_all_gws(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "gateways"
        return self._list_docs(concurrency, True, coll_name, {}, [("gw_name", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_threadpool_list_all_gws_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        return self._list_docs(concurrency, False, "gateways", {}, [("gw_name", ASCENDING)], page_size, None)

    def with_procpool_list_all_gws_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        return self._list_docs(concurrency, True, "gateways", {}, [("gw_name", ASCENDING)], page_size, None)

    def with_threadpool_list_all_vpcs(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, False, coll_name, {}, [("_id", ASCENDING)], self._coll_2_count[coll_name], 1)

    def with_procpool_list_all_vpcs(self, concurrency: int) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, True, coll_name, {}, [("_id", ASCENDING)], self._coll_2_count[coll_name], 1)
    
    def with_threadpool_list_all_vpcs_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, False, coll_name, {}, [("_id", ASCENDING)], page_size, None)

    def with_procpool_list_all_vpcs_rand_page(self, concurrency: int, page_size: int = 100) -> List[QueryPerfResult]:
        coll_name = "vpcs"
        return self._list_docs(concurrency, True, coll_name, {}, [("_id", ASCENDING)], page_size, None)

    def _get_test_funcs(self):
        return (
            self.with_procpool_find_gw,
            self.with_procpool_find_vpc,
            self.with_procpool_list_all_gws,
            self.with_procpool_list_all_vpcs,
            self.with_procpool_list_all_gws_rand_page,
            self.with_procpool_list_all_vpcs_rand_page,
            self.with_procpool_join,
            self.with_procpool_join_rand_page,
            self.with_procpool_join_2nd_last_page,

            self.with_procpool_find_rand_gw,
            self.with_procpool_find_rand_vpc,
            self.with_procpool_list_aws_gws,
            self.with_procpool_list_aws_vpcs,

            # self.with_threadpool_find_gw,
            # self.with_threadpool_find_vpc,
            # self.with_threadpool_list_all_gws,
            # self.with_threadpool_list_all_vpcs,
            # self.with_threadpool_list_all_gws_rand_page,
            # self.with_threadpool_list_all_vpcs_rand_page,
            # self.with_threadpool_join,
            # self.with_threadpool_join_rand_page,
            # self.with_threadpool_join_2nd_last_page,
            # self.with_threadpool_find_rand_gw,
            # self.with_threadpool_find_rand_vpc,
            # self.with_threadpool_list_aws_gws,
            # self.with_threadpool_list_aws_vpcs,
        )

    def stop_all_unrelated_services(self):
        procs_to_stop = "perfmon below rrdcached assetd etcd mongod cloudxd apache2 avx-autoscale spire-controller spire-gateway avx-ctrl-state-sync apache-spiffe-helper avx-ctrl-appserver"
        subprocess.check_call(f"systemctl stop {procs_to_stop}", shell=True)

    # def restore_from_backup(self, backup_path: str):
    #     self.stop_all_unrelated_services()
    #     p = Path("/lib/systemd/system/mymongod.service")
    #     p.write_text(mymongod_service_template)
    #     subprocess.check_call(f"systemctl daemon-reload", shell=True)

    def test(self) -> Dict[str, Result]:
        rtn = {}
        _client = MongoClient(self._db_addr, self._db_port)
        self._dpath.mkdir(mode=0o755, parents=True, exist_ok=True)
        # subprocess.check_call(f"rm -rf {self._dpath.as_posix()}", shell=True)
        for func in self._get_test_funcs():
            res = Result(self._max_concurrency, self._dpath, self._test_name, self._data_scale, str(self._ts), func.__name__)
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
                res.add(concurrency, func(concurrency))
                logger.warning(f"ended {func.__name__} - {concurrency}")
            res.to_csv()
            rtn[func.__name__] = res
        return rtn

if __name__ == "__main__":
    # python3 mongodb_test.py -n test1 -s small -c 3 -d /tmp/result -a localhost -p 8964
    # t = Test('test1', "small", 2, "/tmp/result/", False, "localhost", 8964)
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--test_name", action="store", type=str, required=True)
    parser.add_argument("-s", "--data_scale", action="store", choices=["large", "small"], type=bool, required=True)
    parser.add_argument("-c", "--concurrency", action="store", type=int, default=os.cpu_count() * 2)
    parser.add_argument("-d", "--output_directory_path", action="store", type=str, default="/tmp/result")
    parser.add_argument("-cc", "--clear_cache", action="store_true", type=bool, default=False)
    parser.add_argument("-a", "--db_addr", action="store", type=str, default="localhost")
    parser.add_argument("-p", "--db_port", action="store", type=int, default=8964)
    _args = parser.parse_args(sys.argv[1:])
    t = Test(_args.test_name, _args.data_scale, _args.concurrency, _args.output_directory_path, _args.clear_cache, _args.db_addr, _args.db_port)
    res = t.test()
