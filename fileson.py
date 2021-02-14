import json, os

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
