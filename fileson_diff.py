#!/usr/bin/env python3
from collections import defaultdict
from fileson import Fileson
import argparse, json

parser = argparse.ArgumentParser(description='Find changes from origin database to target')
parser.add_argument('origin', type=str, help='Origin database or directory')
parser.add_argument('target', type=str, help='Target database or directory')
parser.add_argument('delta', nargs='?', type=argparse.FileType('w'), default='-', help='filename for delta or - for stdout (default)')
parser.add_argument('-v', '--verbose', action='count', default=0, help='Verbose status while scanning')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-c', '--checksum', type=str, choices=Fileson.summer.keys(), default=None, help='Checksum method (only for two dirs)')
args = parser.parse_args()

origin = Fileson.load_or_scan(args.origin, checksum=args.checksum)
target = Fileson.load_or_scan(args.target, checksum=args.checksum)

ofiles = {p: o for p,r,o in origin.genItems('all')}
tfiles = {p: o for p,r,o in target.genItems('all')}

checksum = origin.runs[-1]['checksum'] # can be None
if checksum != target.runs[-1]['checksum']:
    if args.verbose:
        print('Different checksum types, existence check unavailable.')
    checksum = None

if args.verbose: print('Using checksum', checksum)

if checksum: # something other than None
    ohash = {o[checksum]: p for p,o in ofiles.items() if isinstance(o, dict)}
    thash = {o[checksum]: p for p,o in tfiles.items() if isinstance(o, dict)}

both = sorted(set(ofiles) | set(tfiles))

if args.verbose: print(len(ofiles), 'in', args.origin, len(tfiles),
        'in', args.target, len(both), 'in both')

deltas = []
for p in both:
    ofile = ofiles.get(p)
    tfile = tfiles.get(p)
    d = {'path': p, 'target': tfile, 'origin': ofile}
    if not ofile:
        if checksum and isinstance(tfile, dict) and tfile[checksum] in ohash:
            d['origin_path'] = ohash[tfile[checksum]]
    elif not tfile:
        if checksum and isinstance(ofile, dict) and ofile[checksum] in thash:
            d['target_path'] = thash[ofile[checksum]]
    elif ofile == tfile: continue # skip appending delta
    deltas.append(d)

json.dump(deltas, args.delta, indent=(2 if args.pretty else None))
