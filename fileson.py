"""Fileson class to manipulate Fileson databases."""
import json, os, time, re
from datetime import datetime
from typing import Any, Tuple, Generator

from logdict import LogDict
from hash import sha_file

# Speed up scanning with scandir in Python 3.5 (or PIP package)
try: from os import scandir
except ImportError: from scandir import scandir

def scantree(path, skip=lambda x: False):
    """Recursively yield DirEntry objects for given directory."""
    for e in scandir(path):
        if skip(e.path): continue
        yield e # the entry itself
        if e.is_dir(follow_symlinks=False):
            yield from scantree(e.path, skip)

def gmt_str(mtime: int=None) -> str:
    """Convert st_mtime to GMT string."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))

def gmt_epoch(mtime: str) -> int:
    """Convert YYYY-MM-DD HH:MM:SS in GMT to epoch."""
    utc_time = datetime.strptime(mtime, '%Y-%m-%d %H:%M:%S')
    return int((utc_time - datetime(1970, 1, 1)).total_seconds())

class Fileson(LogDict):
    """File database with previous versions support based on LogDict.

    The file format is fully compatible so you can use :meth:`LogDict.create`
    to instantiate one. Special keys like :scan:, :checksum: used for metadata
    and additional :meth:`files` and :meth:`dirs` methods expose certain types
    of contents. Also, :meth:`set` used to implement "set if changed"
    functionality.
    """

    summer = {
            'none': lambda p,f: None,
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

    @classmethod
    def load(cls: 'Fileson', dbfile: str) -> 'Fileson':
        """Overloaded class method to support f.fson~1 history syntax."""
        m = re.match(r'(.*)~(\d+)', dbfile)
        if m: dbfile = m.group(1)
        fs = super(Fileson, cls).load(dbfile)
        if m: end = (':scan:', fs[':scan:'] - int(m.group(2)) + 1)
        return fs.slice(None, end) if m else fs

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
            **kwargs: Booleans 'verbose' and 'strict' control behaviour
        """
        checksum = kwargs.get('checksum', None)
        verbose = kwargs.get('verbose', 0)
        skiplist = kwargs.get('skip', [])
        strict = kwargs.get('strict', False)
        make_key = lambda p,f: (p if strict else p.split(os.sep)[-1],
                f['modified_gmt'], f['size'])
        
        # Set metadata for run
        self[':scan:'] = self.get(':scan:', 0) + 1 # first in a scan!
        self[':directory:'] = directory
        self[':checksum:'] = checksum
        self[':date_gmt:'] = gmt_str()

        ccache = {}
        if checksum:
            for p in self.files():
                f = self[p]
                if isinstance(f, dict) and checksum in f:
                    ccache[make_key(p,f)] = f[checksum]

        missing = set(self.files()) | set(self.dirs())
        skip = lambda p: any(pat in p for pat in skiplist)

        startTime, fileCount, byteCount, seenG = time.time(), 0, 0, 0

        if verbose: print('Scanning', directory, 'skipping', skiplist)

        for e in scantree(directory, skip):
            p = os.path.relpath(e.path, directory)
            missing.discard(p)
            if e.is_dir(follow_symlinks=False):
                self.set(p, { 'modified_gmt': gmt_str(e.stat().st_mtime) })
            else:
                f = { 'size': e.stat().st_size,
                      'modified_gmt': gmt_str(e.stat().st_mtime) }

                if checksum:
                    if verbose > 1 and not make_key(p,f) in ccache:
                        print(checksum, p)
                    f[checksum] = ccache.get(make_key(p,f), None) or \
                            Fileson.summer[checksum](e.path, f)

                self.set(p, f)

                if verbose >= 1:
                    fileCount += 1
                    byteCount += f['size']
                    if byteCount // 2**30 > seenG:
                        seenG = byteCount // 2**30
                        secs = time.time() - startTime
                        print(f'{fileCount} files, {seenG:.2f} GiB in {secs}s')

        for p in missing:
            if verbose > 1: print('Removed missing entry', p)
            del self[p] # remove elements not seen this time