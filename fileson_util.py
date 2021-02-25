#!/usr/bin/env python3
from collections import defaultdict
from fileson import Fileson
import argparse, os, sys, json, random

# These are the different argument types that can be added to a command
arg_adders = {
'db_or_dir': lambda p: p.add_argument('db_or_dir', type=str,
    help='Database file or directory, supports db.fson~1 history mode.'),
'dbfile': lambda p: p.add_argument('dbfile', type=str,
    help='Database file (JSON format)'),
'dir': lambda p: p.add_argument('dir', nargs='?', type=str, default=None,
    help='Directory to scan'),
'checksum': lambda p: p.add_argument('-c', '--checksum', type=str,
    choices=Fileson.summer.keys(), default=None,
    help='Checksum method (if relevant in the context)'),
'pretty': lambda p: p.add_argument('-p', '--pretty', action='store_true',
    help='Output indented JSON'),
'strict': lambda p: p.add_argument('-s', '--strict', action='store_true',
    help='Skip checksum only on full path (not just name) match'),
'verbose': lambda p: p.add_argument('-v', '--verbose', action='count',
    default=0, help='Print verbose status. Repeat for even more.'),
'src': lambda p: p.add_argument('src', type=str,
    help='Source DB, use src.fson~1 to access previous version etc.'),
'dest': lambda p: p.add_argument('dest', type=str,
    help='Destination DB'),
'force': lambda p: p.add_argument('-f', '--force', action='store_true',
    help='Force action without additional prompts'),
'minsize': lambda p: p.add_argument('-m', '--minsize', type=str, default='0',
    help='Minimum size (e.g. 100, 10k, 1M)'),
'origin': lambda p: p.add_argument('origin', type=str,
    help='Origin database or directory'),
'target': lambda p: p.add_argument('target', type=str,
    help='Target database or directory'),
'delta': lambda p: p.add_argument('delta', nargs='?',
    type=argparse.FileType('w'), default='-',
    help='filename for delta or - for stdout (default)'),
'percent': lambda p: p.add_argument('-p', '--percent', type=int, default=None,
    help='Percentage of checksums to check'),
'run': lambda p: p.add_argument('-r', '--run', type=int, default=None,
    help='Run number, negative are relative to latest'),
        }

# Function per command
def duplicates(args):
    """Look for duplicates using Fileson DB."""
    minsize = int(args.minsize.replace('G', '000M').replace('M', '000k').replace('k', '000'))

    fs = Fileson.load_or_scan(args.db_or_dir, checksum=args.checksum)
    files = [(p,o) for p,r,o in fs.genItems('files') if o['size'] >= minsize]
    checksum = fs.checksum or 'size'

    if checksum == 'size': print('No checksum, using file size!')
        
    csums = defaultdict(list)
    for p,o in files: csums[o[checksum]].append(p)

    for csum,ps in csums.items():
        if len(ps)>1: print(csum, *ps, sep='\n')
duplicates.args = 'db_or_dir minsize checksum'.split() # args to add

def purge(args):
    """Purge history from Fileson db."""
    if not os.path.exists(args.dbfile):
        print('No such file', args.dbfile)
        return
    if not args.force and not 'y' in \
            input('Are you sure? This cannot be undone. (Y/N) ').lower():
        return
    fs = Fileson.load(args.dbfile)
    fs.purge(args.run or -1)
    fs.save(args.dbfile)
purge.args = 'dbfile force run'.split()

def diff(args):
    """Find changes from origin database to target."""
    origin = Fileson.load_or_scan(args.origin, checksum=args.checksum)
    target = Fileson.load_or_scan(args.target, checksum=args.checksum)
    deltas = origin.diff(target, verbose=args.verbose)
    json.dump(deltas, args.delta, indent=(2 if args.pretty else None))
diff.args = 'origin target delta verbose pretty checksum'.split() # args to add

def stats(args):
    """Show statistics of a Fileson DB."""
    fs = Fileson.load_or_scan(args.db_or_dir)
    print(len(fs.runs), 'runs,', len(fs.root), 'paths, checksum', fs.checksum)
    for r in fs.runs: print('   ', r['date_gmt'],
            r.get('directory', 'no directory specified'))

    files = [i for i in fs.genItems('files')]
    dirs = [i for i in fs.genItems('dirs')]
    size = sum(o['size'] for p,r,o in files)
    print(len(files), 'files', len(dirs), 'directories')

    if dirs: print('Maximum directory depth',
            max(p.count(os.sep) for p,r,o in dirs))

    if files:
        print('Total file size %.2f GiB' % (size/2**30))
        print('Maximum individual file size %.3f GiB' % 
                (max(o['size'] for p,r,o in files)/2**30))
stats.args = ['db_or_dir'] # args to add

def checksum(args):
    """Change or re-run checksums for a Fileson DB."""
    fs = Fileson.load(args.dbfile)
    if args.verbose: print('Existing checksum', fs.checksum)

    if not args.dir:
        if not fs.runs or not 'directory' in fs.runs[-1]:
            print('No directory specified and none in DB!')
            return
        args.dir = fs.runs[-1]['directory']
        if args.verbose: print('Using', args.dir, 'from DB')

    if args.percent: # re-run part of checksums
        files = {p: o for p,r,o in fs.genItems('files')}
        n = args.percent * len(files) // 100
        if args.verbose: print('Rechecking', n, 'files\' checksums')
        csummer = Fileson.summer[fs.checksum]

        for p in random.sample(files.keys(), k=n):
            f = files[p]
            fp = os.path.join(args.dir, p)
            old = f[fs.checksum]
            new = csummer(fp, f)
            if old == new:
                if args.verbose: print('OK', fp.split(os.sep)[-1])
            else:
                print('FAIL', fp)
                print('old', old, 'vs.', new, 'new')
checksum.args = 'dbfile dir percent force verbose'.split() # args to add

def copy(args):
    """Make a copy of (specified version of the) database."""
    if not os.path.exists(args.dest) or args.force or 'y' in \
            input('Do you wish to overwrite target? (Y/N) ').lower():
        fs = Fileson.load(args.src)
        fs.save(args.dest)
copy.args = 'src dest force'.split() # args to add

def scan(args):
    """Create fileson JSON file database."""
    fs = Fileson.load(args.dbfile) if os.path.exists(args.dbfile) \
            else Fileson(checksum=args.checksum)

    if args.checksum != fs.checksum:
        print('Fileson DB has different checksum mode', fs.checksum)
        return

    if not args.dir:
        if not fs.runs or not 'directory' in fs.runs[-1]:
            print('No directory specified and none in DB!')
            return
        args.dir = fs.runs[-1]['directory']
        if args.verbose: print('Using', args.dir, 'from DB')

    fs.scan(args.dir, verbose=args.verbose, strict=args.strict)
    fs.save(args.dbfile, pretty=args.pretty)
scan.args = 'dbfile dir checksum pretty strict verbose'.split() # args to add

# create the top-level parser
parser = argparse.ArgumentParser(description='Fileson database utilities')
subparsers = parser.add_subparsers(help='sub-command help')

# add commands using function metadata and properties
for cmd in (checksum, copy, diff, duplicates, purge, scan, stats):
    cmd.parser = subparsers.add_parser(cmd.__name__, description=cmd.__doc__)
    for argname in cmd.args: arg_adders[argname](cmd.parser)
    cmd.parser.set_defaults(func=cmd)

# parse the args and call whatever function was selected
args = parser.parse_args()
if len(sys.argv)==1: parser.print_help(sys.stderr)
else: args.func(args)
