import json, os, time
from collections import defaultdict
from typing import Any, Tuple, Generator

from hash import sha_file

class Fileson:
    summer = {
            'psm': lambda p,f: f'{p};{f["size"]};{f["modified_gmt"]}',
            'nsm': lambda p,f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}',
            'sha1': lambda p,f: sha_file(p),
            'sha1fast': lambda p,f: sha_file(p, quick=True)+str(f['size']),
            }

    @classmethod
    def load(cls: 'Fileson', filename: str, runsep: str='~') -> 'Fileson':
        if '~' in filename[1:]:
            parts = filename.split(runsep)
            filename, parent = runsep.join(parts[:-1]), int(filename[-1])
        else: parent = None
        with open(filename, 'r', encoding='utf8') as fp:
            js = json.load(fp)
            if not all(k in js for k in ('runs', 'root', 'checksum')): raise \
                    RuntimeError(f"{filename} doesn't seem to be a Fileson DB")
            fs = cls(js['runs'], js['root'], js['checksum'])
            if parent: fs.revert(-parent)
            return fs

    @classmethod
    def load_or_scan(cls: 'Fileson', db_or_dir: str, **kwargs) -> 'Fileson':
        if os.path.isdir(db_or_dir):
            fs = cls(checksum=kwargs.get('checksum', None))
            fs.scan(db_or_dir, **kwargs)
            return fs
        else: return cls.load(db_or_dir)

    def save(self, filename: str, pretty: bool=False) -> None:
        js = {
                'description': 'Fileson file database.',
                'url': 'https://github.com/jokkebk/fileson.git',
                'version': '0.1.1',
                'runs': self.runs,
                'checksum': self.checksum,
                'root': self.root
                }
        with open(filename, 'w', encoding='utf8') as fp:
            json.dump(js, fp, indent=(2 if pretty else None), sort_keys=True)

    def __init__(self, runs: list=None, root: dict={}, checksum: str=None) -> None:
        self.checksum = checksum
        self.runs = runs or []
        # keys are paths, values are (run, type) tuples
        self.root = defaultdict(list, root)

    def set(self, path: str, run: int, obj) -> bool:
        prev = self.root[path][-1][1] if path in self.root else None
        if prev == obj: return False # unmodified
        self.root[path].append((run,obj)) # will become [run,obj] on save
        return True # modified

    def get(self, path: str, run: int=None) -> Tuple[int, Any]:
        """Get current or given run state."""
        if not path in self.root: return (run, None)
        if not run == -1: return self.root[path][-1]
        return next(t for t in self.root[path][::-1] if t[0] <= run)

    def revert(self, run: int) -> None: # discard changes after <run>
        if run < 0: run += len(self.runs)
        if run < 0: raise RuntimeError('No such run!')
        self.runs = self.runs[:run] # discard run history
        for p in list(self.root):
            a = self.root[p]
            while a and a[-1][0] > run: a.pop()
            if not a: del self.root[p]

    def purge(self, run: int=-1) -> None: # discard inactive changes before <run>
        if run < 0: run += len(self.runs)
        for p in list(self.root):
            a = self.root[p]
            b = [(max(r-run,1),o) for r,o in a if r>run]
            if not b: b = [(max(a[-1][0]-run,1), a[-1][1])]
            if len(b) > 1 or b[0][1] != None: self.root[p] = b
            else: del self.root[p] # remove node if only deleted
        self.runs = self.runs[run:]

    def genItems(self, *args: str) -> Generator[str, int, Any]:
        types = set()
        if 'all' in args or 'files' in args: types.add(type({}))
        if 'all' in args or 'dirs' in args: types.add(type('D'))
        if 'all' in args or 'deletes' in args: types.add(type(None))
        for p in self.root:
            r,o = self.root[p][-1]
            if type(o) in types: yield(p,r,o)

    def scan(self, directory: str, **kwargs) -> None:
        verbose = kwargs.get('verbose', 0)
        strict = kwargs.get('strict', False)
        make_key = lambda p: p if strict else p.split(os.sep)[-1]
        
        ccache = {}
        if self.runs and self.checksum:
            for p in self.root:
                r, o = self.root[p][-1]
                if isinstance(o, dict) and self.checksum in o:
                    ccache[(make_key(p), o['modified_gmt'], o['size'])] = \
                        o[self.checksum]

        missing = set(self.root) # remove paths as they are encountered

        self.runs.append({'directory': directory,
            'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())})

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

                if self.checksum:
                    key = (make_key(rpath), modTime, size)
                    if key in ccache: f[self.checksum] = ccache[key]
                    else:
                        if verbose >= 2: print(self.checksum, rpath)
                        f[self.checksum] = Fileson.summer[self.checksum](fpath, f)

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

    def diff(self, comp: 'Fileson', verbose: bool=False) -> list:
        if not self.runs or not comp.runs: raise RuntimeError(('Both '
            'fileson objects need runs to compare differences!'))

        cs = self.checksum if self.checksum == comp.checksum else None

        if cs:
            if verbose: print(f'Using checksum: {cs}')
            ohash = {o[cs]: p for p,r,o in self.genItems('files')}
            thash = {o[cs]: p for p,r,o in comp.genItems('files')}
            pick = lambda f: f # keep checksum
        else:
            if verbose: print('Not using checksum')
            pick = lambda f: {k:f[k] for k in ('size', 'modified_gmt')}

        deltas = []
        for p in sorted(set(self.root) | set(comp.root)):
            _, ofile = self.get(p) # discard run
            _, tfile = comp.get(p) # discard run
            d = {'path': p, 'origin': ofile, 'target': tfile}
            if p == 'some.zip': print(d)
            if not ofile and not tfile: continue
            elif not ofile:
                if cs and isinstance(tfile, dict) and tfile[cs] in ohash:
                    d['origin_path'] = ohash[tfile[cs]]
            elif not tfile:
                if cs and isinstance(ofile, dict) and ofile[cs] in thash:
                    d['target_path'] = thash[ofile[cs]]
            elif isinstance(ofile, dict) and isinstance(tfile, dict): # files
                if pick(ofile) == pick(tfile): continue # skip appending delta
            elif ofile == tfile: continue # simple comparison for non-files
            deltas.append(d) # we got here, so there's a difference
        return deltas
