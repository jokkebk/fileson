#!/usr/bin/env python3
import os, argparse, fileson, json

parser = argparse.ArgumentParser(description='Create fileson JSON file database')
parser.add_argument('dbfile', type=argparse.FileType('w'), help='Database file (JSON format)')
parser.add_argument('dir', nargs='?', type=str, default=os.getcwd(), help='Directory to scan')
parser.add_argument('-c', '--checksum', type=str, choices=fileson.summer.keys(), default=None, help='Checksum method')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-b', '--base', type=str, help='Previous DB to take checksums for unchanged files')
parser.add_argument('-s', '--strict', action='store_true', help='Require full path match with --base')
parser.add_argument('-v', '--verbose', action='count', default=0, help='Print verbose status')
args = parser.parse_args()

fs = fileson.create(args.dir, checksum=args.checksum, base=args.base,
        verbose=args.verbose, strict=args.strict)

json.dump(fs, args.dbfile, indent=(2 if args.pretty else None))
args.dbfile.close()
