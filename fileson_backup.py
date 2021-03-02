#!/usr/bin/env python3
from collections import defaultdict, namedtuple
from fileson import Fileson, gmt_str, gmt_epoch
from logdict import LogDict
from crypt import keygen as kg, AESFile, sha1, calc_etag
import argparse, os, sys, json, signal, time, hashlib, inspect, shutil, re
import boto3

# Return key or if a filename, its contents
def key_or_file(key):
    if isinstance(key, bytes): return key # passthrough
    if os.path.exists(key):
        with open(key, 'r') as f: key = ''.join(f.read().split())
    return bytes.fromhex(key)

# These are the different argument types that can be added to a command
arg_adders = {
'password': lambda p: p.add_argument('password', type=str, help='Password'),
'salt': lambda p: p.add_argument('salt', type=str, help='Salt'),
'input': lambda p: p.add_argument('input', type=str, help='Input file'),
'output': lambda p: p.add_argument('output', type=str, help='Output file'),
'bucket': lambda p: p.add_argument('bucket', type=str, help='S3 bucket'),
'in_obj': lambda p: p.add_argument('in_obj', type=str, help='Input file or S3 object name'),
'out_obj': lambda p: p.add_argument('out_obj', type=str, help='Output file or S3 object name'),
'key': lambda p: p.add_argument('key', type=str,
    help='Key in hex format or filename of the keyfile'),
'keyfile': lambda p: p.add_argument('-k', '--keyfile', type=str,
    help='Key in hex format or filename of the keyfile'),
'partsize': lambda p: p.add_argument('-p', '--partsize', type=int,
    default=8, help='Multipart upload partsize (default 8 matching boto3)'),
'iterations': lambda p: p.add_argument('-i', '--iterations', type=str,
    default='1M', help='PBKDF2 iterations (default 1M)'),
'dbfile': lambda p: p.add_argument('dbfile', type=str,
    help='Database file (JSON format)'),
'logfile': lambda p: p.add_argument('logfile', type=str,
    help='Logfile to append all operations to'),
'source': lambda p: p.add_argument('source', type=str,
    help='Source directory'),
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

def encrypt(args):
    if not args.force and os.path.exists(args.output) and not 'y' in \
            input('Output exists! Do you wish to overwrite? [y/n] '): return
    with AESFile(args.input, 'rb', key_or_file(args.key)) as fin:
        with open(args.output, 'wb') as fout:
            cryptfile(fin, fout, verbose=args.verbose)
encrypt.args = 'input output key verbose force'.split()

def decrypt(args):
    if not args.force and os.path.exists(args.output) and not 'y' in \
            input('Output exists! Do you wish to overwrite? [y/n] '): return
    with open(args.input, 'rb') as fin:
        with AESFile(args.output, 'wb', key_or_file(args.key)) as fout:
            cryptfile(fin, fout, verbose=args.verbose)
decrypt.args = 'input output key verbose force'.split()

def etag(args):
    with open(args.input, 'rb') as f: print(calc_etag(f, args.partsize))
etag.args = 'input partsize'.split()

def upload(args):
    s3 = boto3.client('s3')
    if args.keyfile: fp = AESFile(args.in_obj, 'rb', key_or_file(args.keyfile))
    else: fp = open(args.in_obj, 'rb')
    resp = s3.upload_fileobj(fp, args.bucket, args.out_obj)
    fp.close()
upload.args = 'in_obj out_obj bucket keyfile'.split()

def download(args):
    s3 = boto3.client('s3')
    if args.keyfile: fp = AESFile(args.out_obj, 'wb', key_or_file(args.keyfile))
    else: fp = open(args.out_obj, 'wb')
    resp = s3.download_fileobj(args.bucket, args.in_obj, fp)
    fp.close()
download.args = 'in_obj out_obj bucket keyfile'.split()

def backup(args):
    """Perform backup based on latest Fileson DB state."""
    fs = Fileson.load_or_scan(args.dbfile, checksum='sha1')
    if fs.get(':checksum:', None) != 'sha1':
        print('Backup only works with full SHA1 hash. Safety first.')
        return

    log = Fileson.load(args.logfile)
    log.startLogging(args.logfile)
    log[':backup:'] = log.get(':backup:', 0) + 1
    log[':dbfile:'] = args.dbfile
    log[':date_gmt:'] = gmt_str()
    log[':destination:'] = args.destination

    if args.keyfile:
        key = key_or_file(args.keyfile)
        log[':keyhash:'] = sha1(key).hex()

    m = re.match('s3://(\w+)/(.+)', args.destination)
    if m:
        bucket, folder = m.group(1), m.group(2)
        print(bucket, folder)
        myargs = namedtuple('myargs', 'in_obj out_obj bucket keyfile')
        make_backup = lambda a,b: upload(myargs(a, folder+'/'+b, bucket,
            key if args.keyfile else None))
    else:
        if args.keyfile:
            myargs = namedtuple('myargs', 'input output key verbose force')
            make_backup = lambda a,b: encrypt(myargs(a,
                os.path.join(args.destination, b), key, False, True))
        else: make_backup = lambda a,b: shutil.copyfile(a,
                os.path.join(args.destination, b))

    uploaded = { log[p]['sha1']: p for p in log.files() }

    seed = log[':date_gmt:'] # for backup filename generation
    for p in fs.files():
        o = fs[p]
        if o['sha1'] in uploaded:
            if args.verbose: print('Already uploaded', p)
            continue
        name = sha1(seed+o['sha1']).hex() # deterministic random name
        print('Backup', p.split(os.sep)[-1], o['sha1'], 'to', name)
        make_backup(os.path.join(fs[':directory:'], p), name)
        log[name] = { 'sha1': o['sha1'], 'size': o['size'] }

    log.endLogging()
backup.args = 'dbfile logfile destination keyfile verbose'.split() # args to add

def restore(args):
    """Restore backup based on Fileson DB and backup log."""
    fs = Fileson.load(args.dbfile)
    if fs.get(':checksum:', None) != 'sha1':
        print('Cannot restore without SHA1 hash.')
        return

    log = Fileson.load(args.logfile)

    if args.keyfile:
        key = key_or_file(args.keyfile)
        myargs = namedtuple('myargs', 'input output key verbose force')
        make_restore = lambda a,b: decrypt(myargs(a, b, key, False, True))
        keyhash = sha1(key).hex()
        if keyhash != log[':keyhash:']:
            print(f'Provided key hash {keyhash} does not match backup file!')
            return
    else: make_restore = lambda a,b: shutil.copyfile(a, b)

    uploaded = { log[p]['sha1']: p for p in log.files() }
    for p in sorted(fs.dirs()):
        fp = args.destination
        if p != '.': fp = os.path.join(fp, p)
        print('mkdir', fp)
        os.makedirs(fp, exist_ok=True)
        mtime = gmt_epoch(fs[p]['modified_gmt'])
        os.utime(fp, (mtime, mtime))

    for p in sorted(fs.files()):
        b = uploaded.get(fs[p]['sha1'], None)
        if not b:
            print('Missing', p, fs[p])
            continue
        fp = os.path.join(args.destination, p)
        bp = os.path.join(args.source, b)
        print('get', fp, 'from', bp)
        make_restore(bp, fp)
        mtime = gmt_epoch(fs[p]['modified_gmt'])
        os.utime(fp, (mtime, mtime))
restore.args = 'dbfile logfile source destination keyfile verbose'.split() # args to add

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
