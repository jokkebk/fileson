#!/usr/bin/env python3
import argparse, os
from fileson import Fileson

parser = argparse.ArgumentParser(description='Create fileson JSON file database')
parser.add_argument('dbfile', type=str, help='Database file (JSON format)')
parser.add_argument('dir', nargs='?', type=str, default=None, help='Directory to scan')
parser.add_argument('-c', '--checksum', type=str, choices=Fileson.summer.keys(), default=None, help='Checksum method')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-s', '--strict', action='store_true', help='Skip checksum only on full path (not just name) match')
parser.add_argument('-v', '--verbose', action='count', default=0, help='Print verbose status. Repeat for even more.')
args = parser.parse_args()

fs = Fileson.load(args.dbfile) if os.path.exists(args.dbfile) else Fileson()
fs.scan(args.dir, checksum=args.checksum,
        verbose=args.verbose, strict=args.strict)
fs.save(args.dbfile, pretty=args.pretty)
