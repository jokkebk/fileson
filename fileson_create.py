import os, time, sys, json, argparse, fileson

parser = argparse.ArgumentParser(description='Create jsync file database')
parser.add_argument('dbfile', type=str, help='Database file (JSON format)')
parser.add_argument('dir', nargs='?', type=str, default=os.getcwd(), help='Directory to scan')
parser.add_argument('-c', '--checksum', type=str, choices=fileson.summer.keys(), default='none', help='File checksum')
parser.add_argument('-p', '--pretty', action='store_true', help='Output indented JSON')
parser.add_argument('-b', '--base', type=str, help='Previous DB to take checksums for unchanged files')
parser.add_argument('--verbose', '-v', action='count', default=0, help='Print verbose status')
args = parser.parse_args()

parents = [None]*256
startTime = time.time()
fileCount = 0
byteCount = 0
nextG = 1

csLookup = {} # quick lookup for checksums if --base set
makeKey = lambda f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}'
collisions = set()

if args.base:
    fs = fileson.load(args.base)
    files = fileson.filelist(fs)
    if not files or not args.checksum in files[0]:
        print(args.base, 'has no files or missing', args.checksum, 'checksum')
        exit(-1)
    for f in files:
        key = makeKey(f)
        if key in csLookup: collisions.add(key)
        csLookup[key] = f[args.checksum]
    for c in collisions: del csLookup[c]
    print(len(csLookup), 'checksum lookups',
            len(collisions), 'collisions removed')

hit, miss = 0, 0

for dirName, subdirList, fileList in os.walk(args.dir):
    if args.verbose >= 2: print(dirName)

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
        if args.verbose >= 3: print('  ', fname)
        fullname = os.path.join(dirName, fname)
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = \
            os.stat(fullname)
        modTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))
        fileEntry = {
            'name': fname,
            'size': size,
            'modified_gmt': modTime
        }

        if makeKey(fileEntry) in csLookup:
            hit += 1
            fileEntry[args.checksum] = csLookup[makeKey(fileEntry)]
        else:
            miss += 1
            fileEntry[args.checksum] = fileson.summer[args.checksum](fullname)

        directory['files'].append(fileEntry)

        if args.verbose >= 1:
            fileCount += 1
            byteCount += size
            if byteCount > nextG * 2**30:
                nextG = byteCount // 2**30 + 1;
                elapsed = time.time() - startTime
                print(fileCount, 'files processed',
                        '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

print(hit, 'cache hits', miss, 'misses')

root = next(p for p in parents if p)
root['name'] = '.'; # normalize

run = {
        'description': 'Fileson file database.',
        'url': 'https://github.com/jokkebk/fileson.git',
        'version': '0.0.1',
        'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
        'arguments': sys.argv,
        'dir': args.dir,
        'checksum': args.checksum,
        'root': root
        }

with open(args.dbfile, 'w', encoding='utf8') as fp:
    opts = {'indent': 2} if args.pretty else {}
    json.dump(run, fp, **opts)
