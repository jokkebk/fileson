import json, os, time

from hash import sha_file

class Fileson:
    summer = {
            'psm': lambda p,f: f'{p};{f["size"]};{f["modified_gmt"]}',
            'nsm': lambda p,f: f'{f["name"]};{f["size"]};{f["modified_gmt"]}',
            'sha1': lambda p,f: sha_file(p),
            'sha1fast': lambda p,f: sha_file(p, quick=True)+str(f['size']),
            }

    @staticmethod
    def load(filename):
        with open(filename, 'r', encoding='utf8') as fp:
            js = json.load(fp)
            if not 'version' in fs or not 'root' in fs:
                raise RuntimeError(f'{dbfile} does not seem to be Fileson database')
            return Fileson(js['scans'], js['root'])

    def __init__(self, scans=[], root=[]):
        self.scans = scans
        self.root = {}

    def save(self, filename, pretty=False):
        js = {
                'description': 'Fileson file database.',
                'url': 'https://github.com/jokkebk/fileson.git',
                'version': '0.1.0',
                'scans': self.scans,
                'root': self.root
                }
        with open(filename, 'w', encoding='utf8') as fp:
            json.dump(js, fp, indent=(2 if pretty else None))

    def scan(self, directory, **kwargs):
        checksum = kwargs.get('checksum', None)
        verbose = kwargs.get('verbose', 0)
        strict = kwargs.get('strict', False)
        csummer = Fileson.summer[checksum]
        
        self.scans.append({'checksum': checksum,
            'date_gmt': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            'verbose': verbose, 'strict': strict })

        scan = len(self.scans)

        # Hand crafted entry to get '.' contents to self.root. Fuck this.
        fakeRoot = {'content': {'.': [{'from': 0, 'to': scan, 'type': 'd', 'content': self.root}]}}
        parents = [fakeRoot] + [None]*255
        startTime = time.time()
        fileCount, byteCount, nextG = 0, 0, 1
        for dirName, subdirList, fileList in os.walk(directory):
            path = os.path.relpath(dirName, directory) # relative path
            parts = path.split(os.sep) # parts[-1] is the file/dir name
            parent = parents[len(parts)-1] # fetch parent or fakeRoot

            # Add a node if missing. Key '.' will be added to fakeRoot
            if not parts[-1] in parent['content']:
                print('adding', parts[-1])
                parent['content'][parts[-1]] = [{ 'from': scan, 'to': scan,
                    'type': 'd', 'content': {}}]

            latest = parent['content'][parts[-1]][-1]
            if latest['type'] == 'd':
                dirEntry = latest
                dirEntry['to'] = scan # extend
            else: # file or deleted node replaced with a directory
                dirEntry = {'from': scan, 'to': scan, 'type': 'd', 'content': {}}
                parent['content'][parts[-1]].append(dirEntry)

            parents[len(parts)] = dirEntry # store for children

            continue
            for fname in fileList:
                fpath = os.path.join(dirName, fname)
                rpath = os.path.relpath(fpath, directory) # relative for csLookup
                (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = \
                    os.stat(fpath)
                modTime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mtime))
                fileEntry = {
                    'size': size,
                    'modified_gmt': modTime
                }
                dirEntry['content'][fname] = [(scan, 'f', fileEntry)] # @TODO check if exists

                if checksum:
                    fileEntry['checksum'] = csummer(fpath, fileEntry)

                if verbose >= 1:
                    fileCount += 1
                    byteCount += size
                    if byteCount > nextG * 2**30:
                        nextG = byteCount // 2**30 + 1;
                        elapsed = time.time() - startTime
                        print(fileCount, 'files processed',
                                '%.1f G in %.2f s' % (byteCount/2**30, elapsed))

            #for fname in fileList:

            #    if checksum:
            #        key = csummer(rpath, fileEntry)
            #        #print('Seeking', key, key in csLookup)
            #        if key in csLookup: fileEntry[checksum] = csLookup[key]
            #        else:
            #            fileEntry[checksum] = summer[checksum](fpath, fileEntry)
            #            if verbose >= 2: print(fpath, checksum, fileEntry[checksum])


def load_or_scan(obj, **kwargs): # kwargs only passed to create
    if isinstance(obj, str):
        if os.path.isdir(obj): return create(obj, **kwargs, parents=True)
        fp = sys.stdio if obj=='-' else open(obj, 'r', encoding='utf8')
    fs = json.load(fp)
    fp.close()
