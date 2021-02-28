"""Fileson class to manipulate Fileson databases."""
import json, os, time
from collections import defaultdict
from typing import Any, Tuple, Generator

from logdict import LogDict
from hash import sha_file

def gmt_str(mtime: int=None) -> str:
    """Convert st_mtime to GMT string."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))

class Fileson(LogDict):
    """File database with previous versions support based on LogDict.

    The file format is fully compatible so you can use :meth:`LogDict.create`
    to instantiate one. Special keys like :scan:, :checksum: used for metadata
    and additional :meth:`files` and :meth:`dirs` methods expose certain types
    of contents. Also, :meth:`set` used to implement "set if changed"
    functionality.
    """

    summer = {
            'sha1': lambda p,f: sha_file(p),
            'sha1fast': lambda p,f: sha_file(p, quick=True)+str(f['size']),
            }

    @classmethod
    def load_or_scan(cls: 'Fileson', db_or_dir: str, **kwargs) -> 'Fileson':
        """Load Fileson database or create one by scanning a directory.

        This basically calls :meth:`load` or creates a new
        instance and uses :meth:`scan` after it (passing kwargs).

        Args:
            db_or_dir (str): Database or directory name

        Returns:
            Fileson: New class instance
        """
        if os.path.isdir(db_or_dir):
            fs = cls()
            fs.scan(db_or_dir, **kwargs)
            return fs
        else: return cls.load(db_or_dir)

    def dirs(self) -> list:
        """Return paths to dirs."""
        return [p for p in self if p[0] != ':' and not 'size' in self[p]]

    def files(self) -> list:
        """Return paths to files."""
        return [p for p in self if p[0] != ':' and 'size' in self[p]]

    def set(self, key: Any, val: Any) -> bool:
        """Set key to val if there's a change, in which case return True."""
        if key in self and self[key] == val: return False
        self[key] = val # change will be recorded by LogDict
        return True

    def scan(self, directory: str, **kwargs) -> None:
        """Scan a directory for objects or changes.

        Every invocation creates a new 'run', a version to Fileson
        database. Only changes need to be stored. You can then use
        for example :meth:`genItems` and pick only objects that
        were changed on a given run.

        Args:
            directory (str): Directory to scan
            \*\*kwargs: Booleans 'verbose' and 'strict' control behaviour
        """
        checksum = kwargs.get('checksum', None)
        verbose = kwargs.get('verbose', 0)
        strict = kwargs.get('strict', False)
        make_key = lambda p,f: (p if strict else p.split(os.sep)[-1],
                f['modified_gmt'], f['size'])
        
        # Set metadata for run
        self[':scan:'] = self.get(':scan:', 0) + 1 # first in a scan!
        self[':directory:'] = directory
        self[':checksum:'] = checksum
        self[':date_gmt:'] = gmt_str()

        ccache = {}
        missing = set()
        if checksum:
            for p in self.files():
                missing.add(p)
                f = self[p]
                if isinstance(f, dict) and checksum in f:
                    ccache[make_key(p,f)] = f[checksum]

        startTime = time.time()
        fileCount, byteCount, nextG = 0, 0, 1

        for dirName, subdirList, fileList in os.walk(directory):
            p = os.path.relpath(dirName, directory)
            if self.set(p, { 'modified_gmt': gmt_str(os.stat(dirName).st_mtime) }): print('new dir', p)
            missing.discard(p)

            for fname in fileList:
                fpath = os.path.join(dirName, fname)
                p = os.path.relpath(fpath, directory) # relative for csLookup
                s = os.stat(fpath)
                f = { 'size': s.st_size, 'modified_gmt': gmt_str(s.st_mtime) }

                if checksum:
                    if verbose > 1 and not make_key(p,f) in ccache:
                        print(checksum, p)
                    f[checksum] = ccache.get(make_key(p,f), None) or \
                            Fileson.summer[checksum](fpath, f)

                if self.set(p, f): print('changed', p)
                missing.discard(p)

                if verbose >= 1:
                    fileCount += 1
                    byteCount += f['size']
                    if byteCount > nextG * 2**30:
                        nextG = byteCount // 2**30 + 1;
                        elapsed = time.time() - startTime
                        print(fileCount, 'files processed',
                                '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

        # Mark missing elements as removed (if not already so)
        for p in missing:
            if verbose > 1: print('deleted', p)
            del self[p]

    def diff(self, comp: 'Fileson', verbose: bool=False) -> list:
        """Compare to another Fileson object and report differences.

        Returned delta structure uses 'origin' to refer to self, and
        'target' to refered to the other Fileson object being compared.

        Args:
            comp (Fileson): Target object to compare against
            verbose (bool): Set to True to print out stuff

        Returns:
            list: A list of dict objects outlining the differences.

        Raises:
            RuntimeError: If one of the objects is empty
        """
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
