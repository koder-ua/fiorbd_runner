import contextlib
import sys
import time
import json
import shutil
import argparse
import tempfile
import datetime
import subprocess
from pathlib import Path
from threading import Event, Thread

from typing import List, Any, Generator


def ceph_health_ok():
    try:
        health_js = subprocess.check_output('ceph status -f json', shell=True, timeout=5).decode("utf-8")
        return json.loads(health_js)['health']['status'] == 'HEALTH_OK'
    except TimeoutError:
        return False


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


def run_test(output_dir: Path, cfg: str, opts: Any, pool: str, volume: str, size: int):
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


def rebalance(output_dir: Path, cfg: str, opts: Any, pool: str, volume: str, size: int):
    times = {}
    curr_cfg = cfg.format(QD=opts.qd, POOL=pool, RBD=volume, SIZE=size)
    config_fl = output_dir / "cfg.fio"
    with config_fl.open("w") as fd:
        fd.write(curr_cfg)

    weights_of_nodes = {}
    osd_tree = json.loads(subprocess.check_output("ceph osd tree -f json", shell=True, timeout=10).decode("utf8"))
    names = {"osd.{}".format(osdid): osdid for osdid in opts.osd}

    for node in osd_tree['nodes']:
        if node['name'] in names:
            osdid = names[node['name']]
            assert osdid == node['id']
            assert osdid not in weights_of_nodes
            weights_of_nodes[osdid] = node['crush_weight']

    assert len(weights_of_nodes) == len(names)
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
            while ceph_health_ok():
                time.sleep(0.1)

            next = start
            print("Waiting for rebalance to complete.", end="", flush=True)
            while not ceph_health_ok():
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

    cfg = open(opts.cfg).read()

    if opts.subparser_name == 'fio_test':
        with monitoring(output_dir, opts.monitoring_period):
            run_test(output_dir, cfg, opts, pool, volume, size)
    elif opts.subparser_name == 'rebalance':
        rebalance(output_dir, cfg, opts, pool, volume, size)
    else:
        assert False, opts.subparser_name

    print("Done")
    return 0


if __name__ == "__main__":
    exit(main(sys.argv[1:]))
