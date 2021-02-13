import os, time, sys, json, argparse

from hash import sha_file

parser = argparse.ArgumentParser(description='Create jsync file database')
parser.add_argument('dbfile', type=str, help='Database file (.json appended automatically)')
parser.add_argument('dir', nargs='?', type=str, default=os.getcwd(), help='Directory to scan')
parser.add_argument('-c', '--checksum', type=str, choices=['none', 'sha1', 'sha1fast'], default='none', help='File checksum')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
args = parser.parse_args()

summer = {
        'none': lambda f: 0,
        'sha1': lambda f: sha_file(f),
        'sha1fast': lambda f: sha_file(f, quick=True),
        }

parents = [None]*256

for dirName, subdirList, fileList in os.walk(args.dir):
    parts = dirName.split(os.sep)

    if len(parts)+1 > len(parents):
        print(f'Maximum directory depth {len(parts)} exceeded!')
        exit(-1)

    directory = {
        'name': parts[-1],
        'subdirs': [],
        'files': []
    }

    parents[len(parts)] = directory # store for subdirs

    if parents[len(parts)-1]:
        parents[len(parts)-1]['subdirs'].append(directory)

    for fname in fileList:
        fullname = os.path.join(dirName, fname)
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = \
            os.stat(fullname)
        modTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))
        directory['files'].append({
            'name': fname,
            'size': size,
            #'mtime': mtime,
            'modified_gmt': modTime,
            args.checksum: summer[args.checksum](fullname)
        })

root = next(p for p in parents if p)
root['name'] = '.'; # normalize

run = {
        'description': 'Fileson file database.',
        'url': 'https://github.com/jokkebk/fileson.git',
        'version': '0.0.0',
        'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
        'arguments': sys.argv,
        'dir': args.dir,
        'root': root
        }

with open(args.dbfile, 'w', encoding='utf8') as fp:
    opts = {'indent': 2} if args.pretty else {}
    json.dump(run, fp, **opts)
