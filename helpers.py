import json
import time
import subprocess


def iter_osds(data):
    classes = {}
    for node in data['nodes']:
        if 'device_class' in node:
            classes.setdefault(node['device_class'], []).append(node['id'])

    nodes = {}
    for node in data['nodes']:
        if node.get('type_id') == 2:
            nodes[node['name']] = node['children']

    sata_osds = {}
    for nname, osds in nodes.items():
        sata_osds[nname] = [osd_id for osd_id in osds if osd_id in classes['sata']]

    print("total", sum(map(len, sata_osds.values())), 'sata class osds')

    order = []
    sata_osds = {name: osds for name, osds in sata_osds.items() if osds}
    while sata_osds:
        name, osds = sorted(sata_osds.items(), key=lambda x: len(x[1]))[-1]
        yield name, osds.pop()
        sata_osds = {name: osds for name, osds in sata_osds.items() if osds}


def get_osd_tree():
    # tree_js = subprocess.check_output('ceph osd tree -f json', shell=True)
    tree_js = open('/home/koder/Downloads/tree.json').read()
    return json.loads(tree_js)


def ceph_health_ok():
    health_js = subprocess.check_output('ceph status -f json', shell=True)
    return json.loads(health_js)['health']['status'] == 'HEALTH_OK'


def wait_ok_status():
    print("    Waiting for cluster to be healthy", end="")
    timeouts = [1] * 10
    while not ceph_health_ok():
        time.sleep(timeouts.pop() if timeouts else 10)
        print(".", end="")
    print(" OK")


def remove_class(osd_id):
    print("Removing class of osd {}".format(osd_id))
    cmd = "ceph osd crush rm-device-class osd.{}".format(osd_id)
    print("=>", cmd)
    # subprocess.check_call(cmd, shell=True)


def set_class(osd_id, osd_class):
    print("Setting osd {} class to {}".format(osd_id, osd_class))
    cmd = "ceph osd crush set-device-class {} osd.{}".format(osd_class, osd_id)
    print("=>", cmd)
    # subprocess.check_call(cmd, shell=True)


def main():
    wait_ok_status()
    tree = get_osd_tree()
    osds_to_move = list(iter_osds(tree))[:10]
    first = True
    print("Total {} osds will be moved".format(len(osds_to_move)))
    for node_name, osd_id in osds_to_move:
        if not first:
            print("    Wating 10s before move next osd")
            time.sleep(1)
            print()
            print()
        first = False
        wait_ok_status()
        remove_class(osd_id)
        wait_ok_status()
        set_class(osd_id, 'sata-test')
        wait_ok_status()


if __name__ == "__main__":
    main()
