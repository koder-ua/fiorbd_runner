import contextlib
import sys
import time
import enum
import json
import shutil
import argparse
import tempfile
import datetime
import subprocess
from pathlib import Path
from threading import Event, Thread
from typing import List, Any, Generator, Set, Dict, Iterator


def ceph_health_ok():
    try:
        health_js = subprocess.check_output('ceph status -f json', shell=True, timeout=10).decode("utf-8")
        return json.loads(health_js)['health']['status'] == 'HEALTH_OK'
    except TimeoutError:
        return False


def no_recovery():
    health_js = subprocess.check_output('ceph status -f json', shell=True, timeout=15).decode("utf-8")
    return "PG_DEGRADED" not in json.loads(health_js)['health']['checks']


class CephHealthChecks(enum.Enum):
    PG_DEGRADED = 0
    PG_AVAILABILITY = 1


def ceph_errs() -> Set[CephHealthChecks]:
    health_js = subprocess.check_output(['ceph', 'health', 'detail', '-f', 'json'], timeout=5).decode("utf-8")
    return {getattr(CephHealthChecks, tp) for tp in json.loads(health_js)['checks']}


class OSDTree:
    def __init__(self, data: str) -> None:
        self.osd_tree = json.loads(data)

    def iter_osd(self) -> Iterator[Dict[str, Any]]:
        for node in self.osd_tree['nodes']:
            if node['type'] == 'osd':
                yield node

    @classmethod
    def from_cli(cls) -> 'OSDTree':
        return cls(subprocess.check_output("ceph osd tree -f json", shell=True, timeout=10).decode("utf8"))


def get_osds_for_class(tree: OSDTree, cls_name: str) -> List[int]:
    return [node['id'] for node in tree.iter_osd() if node['device_class'] == cls_name]


def monitoring_func(stop_evt: Event, output_dir: Path, minitoring_period: int = 5):
    next = time.time()
    mon_dir = output_dir / "monitoring"
    mon_dir.mkdir(exist_ok=True)

    while not stop_evt.wait(next - time.time()):
        next = time.time() + minitoring_period
        output_fl = mon_dir / "radosdf_{}.json".format(int(time.time() * 1000))
        try:
            with output_fl.open("wb") as fd:
                subprocess.check_call("rados df -f json-pretty", shell=True, timeout=5, stdout=fd)
        except subprocess.TimeoutExpired:
            print("rados df timeouted")
            output_fl.unlink()


def run_cmd_to(fpath: Path, cmd: str, timeout: int = 10) -> bool:
    try:
        with fpath.open("wb") as fd:
            subprocess.check_call(cmd, shell=True, timeout=timeout, stdout=fd)
    except subprocess.TimeoutExpired:
        print("cmd '{}' failed".format(cmd))
        fpath.unlink()
        return False
    return True


def save_cluster_state(fpath: Path):
    run_cmd_to(fpath, "ceph report")


def run_test(output_dir: Path, opts: Any, pool: str, volume: str, size: int):
    cfg = open(opts.cfg).read()
    for qd in opts.qd:
        print("Starting test with QD = {}".format(qd))
        run_dir = output_dir / str(qd)
        run_dir.mkdir()
        output = run_dir / "fio_output.json"
        bw_log_file = run_dir / "log"
        curr_cfg = cfg.format(QD=qd, BWLOGFILE=bw_log_file, POOL=pool, RBD=volume, SIZE=size)
        config_fl = run_dir / "cfg.fio"
        with config_fl.open("w") as fd:
            fd.write(curr_cfg)
        start_time = time.time()
        cmd = "{fio} '--output-format=json+' --output={output} {config}"
        cmd = cmd.format(fio=opts.fio, config=config_fl, output=output)

        print("Running '{}'".format(cmd))
        run_cmd_to(run_dir / 'before.json', 'ceph report')
        subprocess.check_call(cmd, shell=True)
        print()
        run_cmd_to(run_dir / 'after.json', 'ceph report')

        with (run_dir / 'interval.json').open("w") as fd:
            fd.write(json.dumps([start_time, time.time()]))

        data = json.load(output.open())
        max_p = 0
        for stats in data["jobs"]:
            v = stats['mixed']['clat_ns']['percentile']['{0:.6f}'.format(opts.perc)]
            if v > max_p:
                max_p = v

        max_p /= 10**6
        if max_p > opts.lat_limit:
            print("Latency limit excided at QD={}, lat[{}] = {}".format(qd, opts.perc, max_p))
            break
        else:
            print("Measure {} ppc latency for QD {} = {}".format(opts.perc, qd, max_p))


@contextlib.contextmanager
def monitoring(output_dir: Path, monitoring_period: int) -> Generator[None, None, None]:
    # start monitoring thread
    evt = Event()
    th = Thread(target=monitoring_func, args=(evt, output_dir, monitoring_period))
    th.start()

    try:
        yield
    finally:
        print("Waiting for monitoring thread to finish execution")
        evt.set()
        th.join()


def rebalance(output_dir: Path, opts: Any, pool: str, volume: str, size: int):
    times = {}
    cfg = open(opts.cfg).read()
    curr_cfg = cfg.format(QD=opts.qd, POOL=pool, RBD=volume, SIZE=size)
    config_fl = output_dir / "cfg.fio"
    with config_fl.open("w") as fd:
        fd.write(curr_cfg)

    with (output_dir / "osd_config").open("wb") as fd:
        fd.write(subprocess.check_output(["ceph", "-n", "osd.19", "--show-config"]))

    #osd_tree = OSDTree.from_cli()
    #print(names)
    #weights_of_nodes = {node['id']: node['crush_weight'] for node in osd_tree.iter_osd() if node['name'] in names}

    names = {"osd.{}".format(osdid) for osdid in opts.osd}
    weights_of_nodes = {osdid: 1.85199 for osdid in opts.osd}
    assert len(weights_of_nodes) == len(names), "{}  {}".format(weights_of_nodes, names)
    mon_dir = output_dir / 'monitoring'
    mon_dir.mkdir(exist_ok=True)
    idx = 0
    run_cmd_to(output_dir / 'report.json', 'ceph report')
    for i in range(opts.count):
        for out in [True, False]:
            start = time.time()
            times.setdefault(i, {})["start_{}".format("out" if out else "in")] = int(start)

            for osd_id in opts.osd:
                cmd = "ceph osd crush reweight osd.{} {}".format(osd_id, 0 if out else weights_of_nodes[osd_id])
                print(cmd)
                subprocess.check_call(cmd, shell=True)

            print("Waiting for rebalance to start...")
            while no_recovery():
                time.sleep(0.1)

            next = start
            print("Waiting for rebalance to complete.", end="", flush=True)
            while not no_recovery():
                next += opts.timeout
                output = mon_dir / "{}.json".format(idx)
                idx += 1
                cmd = [opts.fio, '--output-format=json+',  "--output={}".format(output), str(config_fl)]
                subprocess.check_output(cmd)
                dtime = next - time.time()
                if dtime > 0:
                    time.sleep(dtime)
                print(".", end="", flush=True)

            print("OK")
            times[i]["finish_{}".format("out" if out else "in")] = int(time.time())

    with (output_dir / "timings.json").open('w') as fd:
        fd.write(json.dumps(times))


def prepare_output_dir(output_dir: str, wipe: bool) -> Path:
    output_dir_name = tempfile.mkdtemp() if output_dir is None else output_dir

    if "{DATETIME}" in output_dir_name:
        output_dir_name = output_dir_name.format(DATETIME="{0:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now()))

    output_dir = Path(output_dir_name)
    if output_dir and output_dir.exists():
        if not wipe:
            print("Error: Output dir {} already exists. Add --wipe to clear it before test".format(output_dir))
            raise SystemExit(1)
        shutil.rmtree(str(output_dir))

    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    return output_dir


def get_volume_size(pool: str, volume: str) -> int:
    for line in subprocess.check_output("rbd info {}/{}".format(pool, volume), shell=True).decode("utf-8").split("\n"):
        if line.strip().startswith("size"):
            _, sz_s, units, *_ = line.split()
            return int(sz_s) * {"MB": 2 ** 20, "GB": 2 ** 30, "TB": 2 ** 40}[units]
    print("Can't found volume name {}/{}".format(pool, volume))
    raise SystemExit(1)


def parse_opts(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--fio", default='fio')
    parser.add_argument("-w", "--wipe", action="store_true")
    parser.add_argument("-o", "--output-dir", default=None)
    parser.add_argument("-R", "--rbd-volume-name", required=True, help="RBD volume name in format pool/volume")
    parser.add_argument("-m", "--monitoring-period", default=5, type=int)
    parser.add_argument("--unsafe", action="store_true", help="Allow to operate on default pools")

    subparsers = parser.add_subparsers(dest='subparser_name')

    test_parser = subparsers.add_parser('fio_test', help='run normal fio tests')
    test_parser.add_argument("-p", "--perc", default=90, type=int)
    test_parser.add_argument("-l", "--lat-limit", default=20, type=int)
    test_parser.add_argument("-q", "--qd", default=[25, 50, 60, 80, 100, 125, 150, 175, 200], nargs='+', type=int)
    test_parser.add_argument("comment")
    test_parser.add_argument("cfg")

    rebalance_parser = subparsers.add_parser('rebalance', help='Run rebalance and test time and latency')
    rebalance_parser.add_argument("--osd", nargs="+", required=True, type=int, help="OSD id's to rebalance")
    rebalance_parser.add_argument("-q", "--qd", default=1, type=int)
    rebalance_parser.add_argument("-c", "--count", default=5, type=int)
    rebalance_parser.add_argument("--timeout", default=10, type=int)
    rebalance_parser.add_argument("comment")
    rebalance_parser.add_argument("cfg")

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    opts = parse_opts(argv)

    pool, volume = opts.rbd_volume_name.split("/")
    if not opts.unsafe and 'test' not in pool:
        print("Can't work on non-test pool '{}'. Use pool with 'test' in name or add --unsafe".format(pool))
        return 1

    output_dir_name = tempfile.mkdtemp() if opts.output_dir is None else opts.output_dir
    output_dir = prepare_output_dir(output_dir_name, opts.wipe)
    print("Output would be stored into '{}'".format(output_dir))
    (output_dir / "comment").open("w").write(opts.comment)

    size = get_volume_size(pool, volume)

    # save initial info
    run_cmd_to(output_dir / 'rbd_info', "rbd info {}/{}".format(pool, volume))
    run_cmd_to(output_dir / 'rbd_du', "rbd du {}/{}".format(pool, volume))

    if opts.subparser_name == 'fio_test':
        with monitoring(output_dir, opts.monitoring_period):
            run_test(output_dir, opts, pool, volume, size)
    elif opts.subparser_name == 'rebalance':
        rebalance(output_dir, opts, pool, volume, size)
    else:
        assert False, opts.subparser_name

    print("Done")
    return 0


if __name__ == "__main__":
    exit(main(sys.argv[1:]))
