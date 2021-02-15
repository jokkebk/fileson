import json, os, time

from hash import sha_file

summer = {
        'none': lambda f: 0,
        'sha1': lambda f: sha_file(f),
        'sha1fast': lambda f: sha_file(f, quick=True),
        }

def genDirs(fson):
    q = [fson['root']]
    while len(q):
        d = q.pop()
        yield(d)
        q += d['subdirs']

def load(dbfile):
    fp = open(dbfile, 'r', encoding='utf8')
    fs = json.load(fp)
    fp.close()
    if not 'version' in fs or not 'root' in fs:
        raise RuntimeError(f'{dbfile} does not seem to be Fileson database')

    for d in genDirs(fs): # Augment data structure with parents
        for f in d['files']: f['dir'] = d
        for sd in d['subdirs']: sd['parent'] = d

    return fs

def filelist(fson): return [f for d in genDirs(fson) for f in d['files']]

def path(f):
    p, d = [f['name']], f['dir']
    while True:
        p.append(d['name'])
        if not 'parent' in d: break
        d = d['parent']
    return p[::-1]

def create(directory, **kwargs):
    checksum = kwargs.get('checksum', 'sha1')
    verbose = kwargs.get('verbose', 0)
    base = kwargs.get('base', None)

    parents = [None]*256
    startTime = time.time()
    fileCount = 0
    byteCount = 0
    nextG = 1

    csLookup = {} # quick lookup for checksums if --base set
    makeKey = lambda f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}'
    collisions = set()

    if base:
        fs = load(base)

        if fs['checksum'] != checksum: raise RuntimeError(
                f'{base} has no files or missing {checksum} checksum')

        for f in filelist(fs):
            key = makeKey(f)
            if key in csLookup: collisions.add(key)
            csLookup[key] = f[checksum]

        for c in collisions: del csLookup[c]

        if verbose:
            print(len(csLookup), 'checksum lookups',
                len(collisions), 'collisions removed')

    hit, miss = 0, 0

    for dirName, subdirList, fileList in os.walk(directory):
        if verbose >= 2: print(dirName)

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
            if verbose >= 3: print('  ', fname)
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
                fileEntry[checksum] = csLookup[makeKey(fileEntry)]
            else:
                miss += 1
                fileEntry[checksum] = summer[checksum](fullname)

            directory['files'].append(fileEntry)

            if verbose >= 1:
                fileCount += 1
                byteCount += size
                if byteCount > nextG * 2**30:
                    nextG = byteCount // 2**30 + 1;
                    elapsed = time.time() - startTime
                    print(fileCount, 'files processed',
                            '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

    if verbose and csLookup: print(hit, 'cache hits', miss, 'misses')

    root = next(p for p in parents if p)
    root['name'] = '.'; # normalize

    return {
            'description': 'Fileson file database.',
            'url': 'https://github.com/jokkebk/fileson.git',
            'version': '0.0.1',
            'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            'arguments': kwargs,
            'dir': directory,
            'checksum': checksum,
            'root': root
            }

def save(fs, dbfile, pretty=False):
    with open(dbfile, 'w', encoding='utf8') as fp:
        opts = {'indent': 2} if pretty else {}
        json.dump(fs, fp, **opts)
