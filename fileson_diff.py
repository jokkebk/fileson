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

deltas = origin.diff(target, verbose=args.verbose)

json.dump(deltas, args.delta, indent=(2 if args.pretty else None))
