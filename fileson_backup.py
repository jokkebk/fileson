#!/usr/bin/env python3
from collections import defaultdict, namedtuple
from fileson import Fileson, gmt_str
from logdict import LogDict
from crypt import keygen as kg, AESFile, sha1
import argparse, os, sys, json, signal, time, hashlib, inspect, shutil

# Return key or if a filename, its contents
def key_or_file(key):
    if os.path.exists(key):
        with open(key, 'r') as f: key = ''.join(f.read().split())
    return bytes.fromhex(key)

# These are the different argument types that can be added to a command
arg_adders = {
'password': lambda p: p.add_argument('password', type=str, help='Password'),
'salt': lambda p: p.add_argument('salt', type=str, help='Salt'),
'input': lambda p: p.add_argument('input', type=str, help='Input file'),
'output': lambda p: p.add_argument('output', type=str, help='Output file'),
'key': lambda p: p.add_argument('key', type=str,
    help='Key in hex format or filename of the keyfile'),
'keyfile': lambda p: p.add_argument('-k', '--keyfile', type=str,
    help='Key in hex format or filename of the keyfile'),
'iterations': lambda p: p.add_argument('-i', '--iterations', type=str,
    default='1M', help='PBKDF2 iterations (default 1M)'),
'dbfile': lambda p: p.add_argument('dbfile', type=str,
    help='Database file (JSON format)'),
'logfile': lambda p: p.add_argument('logfile', type=str,
    help='Logfile to append all operations to'),
'destination': lambda p: p.add_argument('destination', type=str,
    help='Destination directory'),
'dir': lambda p: p.add_argument('dir', nargs='?', type=str, default=None,
    help='Directory to scan'),
'verbose': lambda p: p.add_argument('-v', '--verbose', action='count',
    default=0, help='Print verbose status. Repeat for even more.'),
'force': lambda p: p.add_argument('-f', '--force', action='store_true',
    help='Force action without additional prompts'),
        }

logfiles = []
def close_logs():
    while logfiles: logfiles.pop().close()

# Function per command
def keygen(args):
    """Create a 32 byte key for AES256 encryption with a password and salt."""
    iterations = int(args.iterations.replace('M', '000k').replace('k', '000'))
    start = time.time()
    keyhex = kg(args.password, args.salt, iterations).hex()
    print(keyhex)
    if args.verbose: print('Generating that took %.3f seconds' % (time.time()-start))
keygen.args = 'password salt iterations verbose'.split()

def cryptfile(infile, outfile, verbose=False):
    startTime, bs = time.time(), 0
    while True:
        data = infile.read(65536)
        if not data: break
        outfile.write(data)
        bs += len(data)
    secs = time.time() - startTime
    if verbose: print('%d b in %.1f s, %.2f GiB/s' % (bs, secs, bs/2**30/secs))

def raw_encrypt(src, dest, key):
    with AESFile(src, 'rb', key) as fin:
        with open(dest, 'wb') as fout:
            cryptfile(fin, fout, verbose=args.verbose)

def raw_decrypt(src, dest, key):
    with open(src, 'rb') as fin:
        with AESFile(dest, 'wb', key) as fout:
            cryptfile(fin, fout, verbose=args.verbose)

def encrypt(args):
    if os.path.exists(args.output) and not 'y' in input('Output exists! '
            'Do you wish to overwrite? [y/n] '): return
    raw_encrypt(args.input, args.output, key_or_file(args.key))
encrypt.args = 'input output key verbose force'.split()

def decrypt(args):
    if os.path.exists(args.output) and not 'y' in input('Output exists! '
            'Do you wish to overwrite? [y/n] '): return
    raw_decrypt(args.input, args.output, key_or_file(args.key))
decrypt.args = 'input output key verbose force'.split()

def run(args):
    """Perform backup based on latest Fileson DB state."""
    fs = Fileson.load_or_scan(args.dbfile, checksum='sha1')
    if fs.get(':checksum:', None) != 'sha1':
        print('Backup only works with full sha1 hash. Safety first.')
        return

    backup = Fileson.load(args.logfile)
    backup.startLogging(args.logfile)
    backup[':backup:'] = backup.get(':backup:', 0) + 1
    backup[':dbfile:'] = args.dbfile
    backup[':date_gmt:'] = gmt_str()
    backup[':destination:'] = args.destination

    if args.keyfile:
        key = key_or_file(args.keyfile)
        make_backup = lambda a,b: raw_encrypt(a, b, key)
    else: make_backup = lambda a,b: shutil.copyfile(a, b)

    uploaded = { backup[p]['sha1']: p for p in backup.files() }

    seed = backup[':date_gmt:'] # for backup filename generation
    for p in fs.files():
        o = fs[p]
        if o['sha1'] in uploaded:
            if args.verbose: print('Already uploaded', p)
            continue
        name = sha1(seed+o['sha1']).hex() # deterministic random name
        print('Backup', p.split(os.sep)[-1], o['sha1'], 'to', name)
        make_backup(os.path.join(fs[':directory:'], p), os.path.join(args.destination, name))
        backup[name] = { 'sha1': o['sha1'], 'size': o['size'] }

    backup.endLogging()

run.args = 'dbfile logfile destination keyfile verbose'.split() # args to add

if __name__ == "__main__":
    # register signal handler to close any open log files
    signal.signal(signal.SIGINT, close_logs)

    # create the top-level parser
    parser = argparse.ArgumentParser(description='Fileson backup utilities')
    subparsers = parser.add_subparsers(help='sub-command help')

    # add commands using function metadata and properties
    for name,cmd in inspect.getmembers(sys.modules[__name__]):
        if inspect.isfunction(cmd) and hasattr(cmd, 'args'):
            cmd.parser = subparsers.add_parser(cmd.__name__, description=cmd.__doc__)
            for argname in cmd.args: arg_adders[argname](cmd.parser)
            cmd.parser.set_defaults(func=cmd)

    # parse the args and call whatever function was selected
    args = parser.parse_args()
    if len(sys.argv)==1: parser.print_help(sys.stderr)
    else: args.func(args)
