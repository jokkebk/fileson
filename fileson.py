import json, os

def genDirs(fson):
    q = [fson['root']]
    while len(q):
        d = q.pop()
        yield(d)
        q += d['subdirs']

def addParents(fson):
    for d in genDirs(fson):
        d['path'] = d['parent']['path'] + os.sep if 'parent' in d else ''
        d['path'] += d['name']
        for sd in d['subdirs']:
            sd['parent'] = d

def filelist(fson):
    files = []
    for d in genDirs(fson):
        files += [{**f, 'dir': d} for f in d['files']]
    return files

def expandPath(d):
    pass
