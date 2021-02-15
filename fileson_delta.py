from collections import defaultdict
import json, argparse, fileson, os

parser = argparse.ArgumentParser(description='Find changes from origin database to target')
parser.add_argument('origin', type=str, help='Origin database or directory')
parser.add_argument('target', type=str, help='Target database or directory')
parser.add_argument('deltafile', nargs='?', type=str, default='--', help='deltafile for delta or -- for stdout (default)')
parser.add_argument('-v', '--verbose', action='count', default=0, help='Print verbose status')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-c', '--checksum', type=str, choices=fileson.summer.keys(), default='sha1fast', help='Checksum method (only for two dirs)')
args = parser.parse_args()

origin, target, checksum = None, None, None

if os.path.isfile(args.origin):
    origin = fileson.load(args.origin)
    checksum = origin['checksum']
if os.path.isfile(args.target):
    target = fileson.load(args.target)
    if checksum and checksum != target['checksum']:
        print('Different checksum types, falling back to date+size+name heuristic')
        checksum = 'none'
    else: checksum = target['checksum']

if not checksum: checksum = args.checksum
if args.verbose: print('Using checksum', checksum)

origin = origin or fileson.create(args.origin, checksum=checksum, parents=True)
target = target or fileson.create(args.target, checksum=checksum, parents=True)

ofiles = fileson.filelist(origin)
tfiles = fileson.filelist(target)

ohash = {} if checksum=='none' else {f[checksum]: f for f in ofiles}
thash = {} if checksum=='none' else {f[checksum]: f for f in tfiles}

filepath = lambda f: os.sep.join(fileson.path(f))
omap = {filepath(f): f for f in ofiles}
tmap = {filepath(f): f for f in tfiles}

both = sorted(set(omap.keys()) | set(tmap.keys()))

if args.verbose: print(len(ofiles), 'in origin,', len(tfiles),
        'in target,', len(both), 'in both')

pick = lambda f: {k: f[k] for k in ('name', 'size', 'modified_gmt', checksum)}

def process(fp, ofile, tfile):
    if ofile == None:
        delta = {'type': 'target only',
                'path': fileson.path(tfile)[:-1],
                'target': pick(tfile)}
        if tfile[checksum] in ohash:
            delta['origin_path'] = fileson.path(ohash[tfile[checksum]])
        return delta
    elif tfile == None:
        delta = {'type': 'origin only',
                'path': fileson.path(ofile)[:-1],
                'origin': pick(ofile)}
        if ofile[checksum] in thash:
            delta['target_path'] = fileson.path(thash[ofile[checksum]])
        return delta
    else:
        for f in (checksum, 'modified_gmt', 'size'):
            if ofile[f] != tfile[f]:
                return {'type': 'change',
                        'path': fileson.path(ofile)[:-1],
                        'origin': pick(ofile),
                        'target': pick(tfile)}
    return None

deltas = []
for fp in both:
    ret = process(fp, omap.get(fp), tmap.get(fp))
    if ret: deltas.append(ret)

deltas = sorted(deltas, key=lambda a: a['type'] + os.sep.join(a['path']))

if args.deltafile == '--':
    print(json.dumps(deltas, indent=(2 if args.pretty else None)))
else:
    fileson.save(deltas, args.deltafile, pretty=args.pretty)
