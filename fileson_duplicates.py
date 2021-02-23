#!/usr/bin/env python3
from collections import defaultdict
from fileson import Fileson
import argparse, os

parser = argparse.ArgumentParser(description='Look for duplicates using Fileson DB')
parser.add_argument('db_or_dir', type=str, help='Database file or directory')
parser.add_argument('-s', '--size-min', type=str, default='0', help='Minimum size (e.g. 100, 10k, 1M)')
parser.add_argument('-c', '--checksum', type=str, choices=Fileson.summer.keys(), default=None, help='Checksum method (directory mode only)')
args = parser.parse_args()

minsize = int(args.size_min.replace('G', '000M').replace('M', '000k').replace('k', '000'))

fs = Fileson.load_or_scan(args.db_or_dir, checksum=args.checksum)
files = [(p,o) for p,r,o in fs.genItems('files') if o['size'] >= minsize]
checksum = fs.runs[-1]['checksum']

if not checksum:
    print('No checksum, using file size!')
    checksum = 'size'
    
csums = defaultdict(list)
for p,o in files: csums[o[checksum]].append(p)

for csum in [cs for cs in csums if len(csums[cs]) > 1]:
    print(csum)
    for p in csums[csum]: print(p)
