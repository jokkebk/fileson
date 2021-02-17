#!/usr/bin/env python3
from collections import defaultdict
import json, argparse, fileson, os, sys

parser = argparse.ArgumentParser(description='Look for duplicates using Fileson DB')
parser.add_argument('db_or_dir', type=str, help='Database file or directory')
parser.add_argument('-s', '--size-min', type=str, default='0', help='Minimum size (e.g. 100, 10k, 1M)')
parser.add_argument('-c', '--checksum', type=str, choices=fileson.summer.keys(), default=None, help='Checksum method (directory mode only)')
args = parser.parse_args()

minsize = int(args.size_min.replace('G', '000M').replace('M', '000k').replace('k', '000'))

fs = fileson.load(args.db_or_dir, checksum=args.checksum)

files = [f for f in fileson.filelist(fs) if f['size'] >= minsize]
checksum = fs['checksum']

if not files: print('No files.')
if not checksum:
    print('No checksum, using file size!')
    checksum = 'size'
    
csums = defaultdict(list)
for f in files: csums[f[checksum]].append(f)

for csum in [cs for cs in csums if len(csums[cs]) > 1]:
    files = csums[csum]
    print(csum)
    for f in csums[csum]: print(os.sep.join(fileson.path(f)))
