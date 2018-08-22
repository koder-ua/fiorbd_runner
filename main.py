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

from typing import List, Any, Dict


def parse_opts(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fio", default='fio')
    parser.add_argument("-p", "--perc", default=90, type=int)
    parser.add_argument("-w", "--wipe", action="store_true")
    parser.add_argument("-l", "--lat-limit", default=20, type=int)
    parser.add_argument("-m", "--monitoring-period", default=5, type=int)
    parser.add_argument("-q", "--qd", default=[25, 50, 60, 80, 100, 125, 150, 175, 200], nargs='+', type=int)
    parser.add_argument("-o", "--output-dir", default=None)
    parser.add_argument("comment")
    parser.add_argument("cfg")
    return parser.parse_args(argv)


def monitoring_func(stop_evt: Event, output_dir: Path, minitoring_period: int = 5):
    next = time.time()
    mon_dir = output_dir / "monitoring"
    mon_dir.mkdir()

    while not stop_evt.wait(next - time.time()):
        next = time.time() + minitoring_period
        output_fl = mon_dir / "radosdf_{}.json".format(int(time.time() * 1000))
        try:
            with output_fl.open("wb") as fd:
                subprocess.check_call("rados df -f json-pretty", shell=True, timeout=5, stdout=fd)
        except subprocess.TimeoutExpired:
            print("rados df timeouted")
            output_fl.unlink()


def save_cluster_state(fpath: Path):
    try:
        with fpath.open("wb") as fd:
            subprocess.check_call("ceph report", shell=True, timeout=10, stdout=fd)
    except subprocess.TimeoutExpired:
        print("ceph report timeouted")
        fpath.unlink()


def main(argv: List[str]) -> int:
    opts = parse_opts(argv)

    cfg = open(opts.cfg).read()

    output_dir_name = tempfile.mkdtemp() if opts.output_dir is None else opts.output_dir

    if "{DATETIME}" in output_dir_name:
        output_dir_name = output_dir_name.format(DATETIME="{0:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now()))

    output_dir = Path(output_dir_name)
    if opts.output_dir and output_dir.exists():
        if not opts.wipe:
            print("Error: Output dir {} already exists. Add --wipe to clear it before test".format(output_dir))
            return 1
        shutil.rmtree(str(output_dir))
        output_dir.mkdir(parents=True)

    print("Output would be stored into '{}'".format(output_dir))
    (output_dir / "comment").open("w").write(opts.comment)

    # start monitoring thread
    evt = Event()
    th = Thread(target=monitoring_func, args=(evt, output_dir, opts.monitoring_period))
    th.start()

    try:
        for qd in opts.qd:
            print("Starting test with QD = {}".format(qd))
            run_dir = output_dir / str(qd)
            run_dir.mkdir()
            output = run_dir / "fio_output.json"
            bw_log_file = run_dir / "log"
            curr_cfg = cfg.format(QD=qd, BWLOGFILE=bw_log_file)
            config_fl = run_dir / "cfg.fio"
            with config_fl.open("w") as fd:
                fd.write(curr_cfg)
            start_time = time.time()
            cmd = "{fio} '--output-format=json+' --output={output} {config}"
            cmd = cmd.format(fio=opts.fio, config=config_fl, output=output)

            print("Running '{}'".format(cmd))
            save_cluster_state(run_dir / 'before.json')
            subprocess.check_call(cmd, shell=True)
            print()
            save_cluster_state(run_dir / 'after.json')

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
    finally:
        print("Waiting for monitoring thread to finish execution")
        evt.set()
        th.join()


    print("Done")
    return 0


if __name__ == "__main__":
    exit(main(sys.argv[1:]))
