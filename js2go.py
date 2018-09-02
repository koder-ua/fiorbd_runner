import re
import sys
import json
from typing import Dict, List, Any, Tuple



def name2go(name: str) -> Tuple[bool, str]:
    if '_' in name:
        return True, "".join(chunk.capitalize() for chunk in name.split('_'))
    return False, name.capitalize()


def name2rec(name: str, tp: str) -> str:
    need_tag, go_name = name2go(name)
    if need_tag:
        return f'{go_name} {tp} `json:"{name}"`'
    return f'{go_name} {tp}'


tpmap = {
    int: 'int',
    float: 'float',
    str: 'string'
}


def dict2go(obj: Dict[str, Any], idx: List[int], tablen: int = 4) -> Tuple[str, List[str]]:
    structs: List[List[str]] = []

    name = f"Struct{idx[0]}"
    main: List[str] = [f"type {name} struct{{"]
    idx[0] += 1

    prefix = " " * tablen
    for key, val in obj.items():
        if type(val) in tpmap:
            if isinstance(val, str):
                if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}", val):
                    tp = 'CephTime'
                else:
                    tp = 'string'
            else:
                tp = tpmap[type(val)]
            main.append(prefix + name2rec(key, tp))
        elif isinstance(val, list):
            all_types = set(type(vl) for vl in val)
            if len(all_types) == 0:
                tp = 'interface{}'
            else:
                assert len(all_types) == 1
                first_tp = next(iter(all_types))
                assert first_tp in tpmap
                tp = tpmap[first_tp]
            main.append(prefix + name2rec(key, '[]' + tp))
        elif isinstance(val, dict):
            name, definition = dict2go(val, idx, tablen)
            main.append(prefix + name2rec(key, name))
            structs.append(definition)

    structs.append(main + ['}'])

    res = []

    for lst in structs:
        res.extend(lst)
        res.append("")

    return name, res

print("\n".join(dict2go(json.load(open(sys.argv[1])), [0])[1]))
