import os, argparse, fileson

parser = argparse.ArgumentParser(description='Create jsync file database')
parser.add_argument('dbfile', type=str, help='Database file (JSON format)')
parser.add_argument('dir', nargs='?', type=str, default=os.getcwd(), help='Directory to scan')
parser.add_argument('-c', '--checksum', type=str, choices=fileson.summer.keys(), default='none', help='File checksum')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-b', '--base', type=str, help='Previous DB to take checksums for unchanged files')
parser.add_argument('--verbose', '-v', action='count', default=0, help='Print verbose status')
args = parser.parse_args()

fs = fileson.create(args.dir, checksum=args.checksum, base=args.base,
        verbose=args.verbose)

fileson.save(fs, args.dbfile, pretty=args.pretty)
