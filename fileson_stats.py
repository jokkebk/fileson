#!/usr/bin/env python3
from collections import defaultdict
import json, argparse, fileson, os

parser = argparse.ArgumentParser(description='Show statistics on Fileson DB')
parser.add_argument('db_or_dir', type=str, help='Database file or directory')
args = parser.parse_args()

fs = fileson.load(args.db_or_dir)
files = fileson.filelist(fs)
dirs = [d for d in fileson.genDirs(fs)]

print(len(files), 'files', len(dirs), 'directories')
print('Maximum directory depth', max(len(fileson.path(d)) for d in dirs))
