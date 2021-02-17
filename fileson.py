import json, os, time

from hash import sha_file

summer = {
        'psm': lambda p,f: f'{p};{f["size"]};{f["modified_gmt"]}',
        'nsm': lambda p,f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}',
        'sha1': lambda p,f: sha_file(p),
        'sha1fast': lambda p,f: sha_file(p, quick=True)+str(f['size']),
        }

def genDirs(fs):
    q = [fs['root']]
    while len(q):
        d = q.pop()
        yield(d)
        q += d['subdirs']

def addParents(fs):
    fs['root']['parent'] = None
    for d in genDirs(fs): # Augment data structure with parents
        for f in d['files']: f['parent'] = d
        for sd in d['subdirs']: sd['parent'] = d

def filelist(fs): return [f for d in genDirs(fs) for f in d['files']]

def path(o):
    p = []
    while o:
        p.append(o['name'])
        o = o['parent']
    return p[::-1]

def create(directory, **kwargs):
    checksum = kwargs.get('checksum', None)
    verbose = kwargs.get('verbose', 0)
    base = kwargs.get('base', None)
    haveParents = kwargs.get('parents', False)

    parents = [None]*256
    startTime = time.time()
    fileCount = 0
    byteCount = 0
    nextG = 1

    csLookup = {} # quick lookup for checksums if --base set
    makeKey = lambda f: summer['nsm']('', f)
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

    for dirName, subdirList, fileList in os.walk(directory):
        if verbose >= 2: print(dirName)

        parts = dirName.split(os.sep)

        if len(parts)+1 > len(parents):
            print(f'Maximum directory depth {len(parts)} exceeded!')
            exit(-1)

        dirEntry = {
            'name': parts[-1],
            'subdirs': [],
            'files': []
        }

        parents[len(parts)] = dirEntry # store for subdirs

        if parents[len(parts)-1]:
            parents[len(parts)-1]['subdirs'].append(dirEntry)

        for fname in fileList:
            if verbose >= 3: print('  ', fname)
            path = os.path.join(dirName, fname)
            (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = \
                os.stat(path)
            modTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))
            fileEntry = {
                'name': fname,
                'size': size,
                'modified_gmt': modTime
            }

            if checksum:
                if makeKey(fileEntry) in csLookup:
                    fileEntry[checksum] = csLookup[makeKey(fileEntry)]
                else:
                    fileEntry[checksum] = summer[checksum](path, fileEntry)
                    if verbose >= 2: print(path, fileEntry[checksum])

            dirEntry['files'].append(fileEntry)

            if verbose >= 1:
                fileCount += 1
                byteCount += size
                if byteCount > nextG * 2**30:
                    nextG = byteCount // 2**30 + 1;
                    elapsed = time.time() - startTime
                    print(fileCount, 'files processed',
                            '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

    root = next(p for p in parents if p)
    root['name'] = '.'; # normalize

    fs = {
            'description': 'Fileson file database.',
            'url': 'https://github.com/jokkebk/fileson.git',
            'version': '0.0.1',
            'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            'arguments': kwargs,
            'dir': directory,
            'checksum': checksum,
            'root': root
            }
    if haveParents: addParents(fs)
    return fs

def load(obj, **kwargs): # kwargs only passed to create
    if isinstance(obj, str):
        if os.path.isdir(obj): return create(obj, **kwargs, parents=True)
        fp = sys.stdio if obj=='-' else open(obj, 'r', encoding='utf8')
    fs = json.load(fp)
    fp.close()
    if not 'version' in fs or not 'root' in fs:
        raise RuntimeError(f'{dbfile} does not seem to be Fileson database')
    addParents(fs)
    return fs
