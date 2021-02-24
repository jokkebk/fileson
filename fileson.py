import json, os, time
from collections import defaultdict

from hash import sha_file

class Fileson:
    summer = {
            'psm': lambda p,f: f'{p};{f["size"]};{f["modified_gmt"]}',
            'nsm': lambda p,f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}',
            'sha1': lambda p,f: sha_file(p),
            'sha1fast': lambda p,f: sha_file(p, quick=True)+str(f['size']),
            }

    @classmethod
    def load(cls, filename):
        with open(filename, 'r', encoding='utf8') as fp:
            js = json.load(fp)
            if not 'runs' in js or not 'root' in js:
                raise RuntimeError(f'{dbfile} does not seem to be Fileson database')
            return cls(js['runs'], defaultdict(list, js['root']))

    @classmethod
    def load_or_scan(cls, obj, **kwargs): # kwargs only passed to scan
        if os.path.isdir(obj):
            fs = cls()
            fs.scan(obj, **kwargs)
            return fs
        else: return cls.load(obj)

    def save(self, filename, pretty=False):
        js = {
                'description': 'Fileson file database.',
                'url': 'https://github.com/jokkebk/fileson.git',
                'version': '0.1.0',
                'runs': self.runs,
                'root': self.root
                }
        with open(filename, 'w', encoding='utf8') as fp:
            json.dump(js, fp, indent=(2 if pretty else None), sort_keys=True)

    def __init__(self, runs=None, root=None):
        self.runs = runs or []
        # keys are paths, values are (run, type) tuples
        self.root = root or defaultdict(list)

    def set(self, path, run, obj):
        prev = self.root[path][-1][1] if path in self.root else None
        if prev == obj: return False # unmodified
        self.root[path].append((run,obj)) # will become [run,obj] on save
        return True # modified

    def get(self, path, run=None): # get current or given run state
        if not path in self.root: return (run, None)
        if not run == -1: return self.root[path][-1]
        return next(t for t in self.root[path][::-1] if t[0] <= run)

    def genItems(self, *args):
        types = set()
        if 'all' in args or 'files' in args: types.add(type({}))
        if 'all' in args or 'dirs' in args: types.add(type('D'))
        if 'all' in args or 'deletes' in args: types.add(type(None))
        for p in self.root:
            r,o = self.root[p][-1]
            if type(o) in types: yield(p,r,o)

    def scan(self, directory, **kwargs):
        checksum = kwargs.get('checksum', None)
        verbose = kwargs.get('verbose', 0)
        strict = kwargs.get('strict', False)
        make_key = lambda p: p if strict else p.split(os.sep)[-1]
        
        ccache = {}
        if self.runs and checksum:
            for p in self.root:
                r, o = self.root[p][-1]
                if isinstance(o, dict) and checksum in o:
                    ccache[(make_key(p), o['modified_gmt'], o['size'])] = \
                        o[checksum]
        #for p in ccache: print(p, ccache[p])

        missing = set(self.root) # remove paths as they are encountered

        self.runs.append({'checksum': checksum,
            'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            'verbose': verbose, 'strict': strict })

        run = len(self.runs)

        startTime = time.time()
        fileCount, byteCount, nextG = 0, 0, 1

        for dirName, subdirList, fileList in os.walk(directory):
            path = os.path.relpath(dirName, directory) # relative path
            self.set(path, run, 'D')
            missing.discard(path)

            for fname in fileList:
                fpath = os.path.join(dirName, fname)
                rpath = os.path.relpath(fpath, directory) # relative for csLookup

                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = \
                    os.stat(fpath)
                modTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))
                f = {
                    'size': size,
                    'modified_gmt': modTime
                }

                if checksum:
                    key = (make_key(rpath), modTime, size)
                    if key in ccache: f[checksum] = ccache[key]
                    else: f[checksum] = Fileson.summer[checksum](fpath, f)

                if self.set(rpath, run, f): pass # print('modified', rpath)
                missing.discard(rpath)

                if verbose >= 1:
                    fileCount += 1
                    byteCount += size
                    if byteCount > nextG * 2**30:
                        nextG = byteCount // 2**30 + 1;
                        elapsed = time.time() - startTime
                        print(fileCount, 'files processed',
                                '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

        # Mark missing elements as removed (if not already so)
        for p in missing: self.set(p, run, None) 

    def diff(self, comp, verbose=False):
        if not self.runs or not comp.runs: raise RuntimeError(('Both '
            'fileson objects need runs to compare differences!'))

        checksum = self.runs[-1]['checksum']
        if checksum != comp.runs[-1]['checksum']: checksum = None

        if checksum:
            if verbose: print(f'Using checksum: {checksum}')
            ohash = {o[checksum]: p for p,r,o in self.genItems('files')}
            thash = {o[checksum]: p for p,r,o in comp.genItems('files')}
            pick = lambda f: f # keep checksum
        else:
            if verbose: print('Not using checksum')
            pick = lambda f: {k:f[k] for k in ('size', 'modified_gmt')}

        deltas = []
        for p in sorted(set(self.root) | set(comp.root)):
            _, ofile = self.get(p) # discard run
            _, tfile = comp.get(p) # discard run
            d = {'path': p, 'origin': ofile, 'target': tfile}
            if not isinstance(ofile, dict) or not isinstance(tfile, dict):
                if ofile == tfile: continue # simple comparison for non-files
            if not ofile:
                if checksum and tfile[checksum] in ohash:
                    d['origin_path'] = ohash[tfile[checksum]]
            elif not tfile:
                if checksum and ofile[checksum] in thash:
                    d['target_path'] = thash[ofile[checksum]]
            elif pick(ofile) == pick(tfile): continue # skip appending delta
            deltas.append(d) # we got here, so there's a difference
        return deltas
