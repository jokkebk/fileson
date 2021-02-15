from collections import defaultdict
import json, argparse, fileson, os

parser = argparse.ArgumentParser(description='Find changes from origin database to target')
parser.add_argument('origin', type=str, help='Origin database')
parser.add_argument('target', type=str, help='Target database')
args = parser.parse_args()

origin = fileson.load(args.origin, paths=True)
target = fileson.load(args.target, paths=True)

checksum = origin['checksum']
if checksum != target['checksum']:
    print('Different checksum types, falling back to date+size+name heuristic')
    checksum = 'none'

filepath = lambda f: f['dir']['path'] + os.sep + f['name']

ofiles = fileson.filelist(origin)
tfiles = fileson.filelist(target)

ohash = {} if checksum=='none' else {f[checksum]: f for f in ofiles}

omap = {filepath(f): f for f in ofiles}
tmap = {filepath(f): f for f in tfiles}

both = sorted(set(omap.keys()) | set(tmap.keys()))

print(len(ofiles), 'in origin,', len(tfiles),
        'in target,', len(both), 'in both')

def process(fp, ofile, tfile):
    if ofile == None:
        if tfile[checksum] in ohash:
            f = ohash[tfile[checksum]]
            return {'action': 'copy',
                    'origin': filepath(f), 'file': fp }
        else: return {'action': 'create', 'file': fp }
    elif tfile == None:
        return {'action': 'delete', 'file': fp}
    else:
        diff = []
        for f in (checksum, 'modified_gmt', 'size'):
            if ofile[f] != tfile[f]: diff.append(f)
        if diff: return {'action': 'modify', 'file': fp, 'changes': diff}
    return None

actions = []
for fp in both:
    ret = process(fp, omap.get(fp), tmap.get(fp))
    if ret: actions.append(ret)

for a in sorted(actions, key=lambda a: a['action'] + a['file']):
    print(a)
