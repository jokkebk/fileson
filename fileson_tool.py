#!/usr/bin/env python3
from collections import namedtuple
import argparse, configparser, csv, datetime, inspect, os, sys

from fileson import Fileson
from fileson_util import scan as util_scan
from fileson_backup import backup as util_backup, etag as util_etag

config = configparser.ConfigParser()
if not config.read('fileson.ini'):
    print('You need to create a fileson.ini in this directory first!')
    exit(-1)

# Function per command
def check(args):
    """Run a check on configuration and directories."""
    for entry in args.entry or config.sections():
        conf = config[entry]
        fileson = f'{entry}.fson'
        logfile = f'{entry}.log'
        print(f'[{entry}]')

        print('Folder', conf['folder'], 'exists!' \
           if os.path.isdir(conf['folder']) \
               else 'missing!')
        print('Fileson', fileson, 'exists!' \
           if os.path.isfile(fileson) else 'missing!')
        print('Backup log', logfile, 'exists!' \
           if os.path.isfile(logfile) else 'missing!')
        if 'skip' in conf:
            print('Skip', conf['skip'].split('\n'))
        
        print()
check.args = 'entry verbose'.split() # args to add

def scan(args):
    """Scan one entry or all."""
    for entry in args.entry or config.sections():
        conf = config[entry]
        fileson = f'{entry}.fson'
        skip = conf['skip'].split('\n') if 'skip' in conf else []
        strict = config.getboolean(entry, 'strict', fallback=False)
        checksum = conf.get('checksum', 'sha1')
        
        print(f'Scanning {entry}...')

        myargs = namedtuple('myargs', 'dbfile dir checksum simulate skip strict verbose')
        util_scan(myargs(fileson, config[entry]['folder'], checksum, False, skip, strict, args.verbose))
scan.args = 'entry verbose'.split() # args to add

def backup(args):
    """Backup one entry or all."""
    for entry in args.entry or config.sections():
        conf = config[entry]
        fileson = f'{entry}.fson'
        logfile = f'{entry}.log'
        destination = conf['destination']
        keyfile = conf['key']
        deep_archive = config.getboolean(entry, 'deep_archive', fallback=False)
        
        destination = destination.replace('$ENTRY$', entry)
        destination = destination.replace('$DATE$', str(datetime.datetime.today()).split()[0])

        myargs = namedtuple('myargs', 'dbfile logfile destination keyfile deep_archive simulate verbose')
        args = myargs(fileson, logfile, destination, keyfile, deep_archive, args.simulate, args.verbose)
        
        print(f'Backing up {entry} to {destination}...')
        if args.verbose > 1: print(args)
        util_backup(args)
backup.args = 'entry simulate verbose'.split() # args to add

def etag(args):
    """Check ETAG checksums of backed up files."""
    etags = {}
    with open(args.csv, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        I = 0
        for row in reader:
            fname, etag = row[1].split('/')[-1], row[4]
            etags[fname] = etag
    print('Read', len(etags), 'etags from', args.csv)

    for entry in args.entry or config.sections():
        print(f'Processing {entry}')
        log = Fileson.load(f'{entry}.log')
        
        ok, missing = 0, 0
        for e,f in [(k,log[k]) for k in log.files()]:
            if not 'etag' in f: continue
            if not e in etags:
                missing += 1
                continue
            if f['etag'] == etags[e]: ok += 1
            else: print('Mismatching etag', e, f, etags[e])
        print(ok, 'ok,', missing, 'not in CSV (yet)')
etag.args = 'csv entry partsize verbose'.split() # args to add

if __name__ == "__main__":
    # These are the different argument types that can be added to a command
    arg_adders = {
    'csv': lambda p: p.add_argument('csv', type=str, help='CSV file with etags'),
    'entry': lambda p: p.add_argument('-e', '--entry', type=str, nargs='?', action='append',
        help='Backup entry (repeat for multiple) in fileson.ini'),
    'verbose': lambda p: p.add_argument('-v', '--verbose', action='count',
        default=0, help='Print verbose status. Repeat for even more.'),
    'force': lambda p: p.add_argument('-f', '--force', action='store_true',
        help='Force action without additional prompts'),
    'partsize': lambda p: p.add_argument('-p', '--partsize', type=int,
        default=None, help='Multipart upload partsize (default 8 matching boto3)'),
    'simulate': lambda p: p.add_argument('-i', '--simulate', action='store_true',
        help='Simulate only (no saving)'),
            }

    # create the top-level parser
    parser = argparse.ArgumentParser(description='Fileson backup tool')
    subparsers = parser.add_subparsers(help='sub-command help')

    # add commands using function metadata and properties
    for name,cmd in inspect.getmembers(sys.modules[__name__]):
        if inspect.isfunction(cmd) and hasattr(cmd, 'args') \
                and cmd.__module__ == __name__:
            cmd.parser = subparsers.add_parser(cmd.__name__, description=cmd.__doc__)
            for argname in cmd.args: arg_adders[argname](cmd.parser)
            cmd.parser.set_defaults(func=cmd)

    # parse the args and call whatever function was selected
    args = parser.parse_args()
    if len(sys.argv)==1: parser.print_help(sys.stderr)
    else: args.func(args)
