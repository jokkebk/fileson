import json, os

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

def load(dbfile, parents=False, paths=False):
    fp = open(dbfile, 'r', encoding='utf8')
    fs = json.load(fp)
    fp.close()
    if not 'version' in fs or not 'root' in fs:
        raise RuntimeError(f'{dbfile} does not seem to be Fileson database')

    if fs['version'] == '0.0.0':
        fs['checksum'] = 'none'
        for d in genDirs(fs):
            if d['files']:
                fs['checksum'] = next(f for f in summer.keys() \
                        if f in d['files'][0])
                break
        print('Augmented', dbfile, 'with checksum:', fs['checksum'])

    for d in genDirs(fs):
        if paths:
            d['path'] = d['parent']['path'] + os.sep if 'parent' in d else ''
            d['path'] += d['name']
        if paths or parents:
            for sd in d['subdirs']: sd['parent'] = d

    return fs

def filelist(fson):
    files = []
    for d in genDirs(fson):
        files += [{**f, 'dir': d} for f in d['files']]
    return files
