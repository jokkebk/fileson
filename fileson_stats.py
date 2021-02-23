#!/usr/bin/env python3
from collections import defaultdict
from fileson import Fileson
import argparse, os

parser = argparse.ArgumentParser(description='Show statistics on Fileson DB')
parser.add_argument('db_or_dir', type=str, help='Database file or directory')
args = parser.parse_args()

fs = Fileson.load_or_scan(args.db_or_dir)
files = [i for i in fs.genItems('files')]
dirs = [i for i in fs.genItems('dirs')]

print(len(files), 'files', len(dirs), 'directories')
if dirs:
    print('Maximum directory depth',
            max(p.count(os.sep) for p,r,o in dirs))
if files:
    print('Maximum file size %.3f GB' % 
            (max(o['size'] for p,r,o in files)/2**30))
