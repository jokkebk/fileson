from collections import defaultdict
import json, argparse, fileson, os

parser = argparse.ArgumentParser(description='Look for duplicates in Fileson DB')
parser.add_argument('dbfile', type=str, help='Database file')
parser.add_argument('-s', '--size-min', type=str, default='0', help='Minimum size (e.g. 100, 10k, 1M)')
args = parser.parse_args()

minsize = int(args.size_min.replace('G', '000M').replace('M', '000k').replace('k', '000'))

fs = fileson.load(args.dbfile)
files = [f for f in fileson.filelist(fs) if f['size'] >= minsize]
checksum = fs['checksum']

if not files: print('No files.')
if checksum == 'none': print('No checksum!')
else:
    csums = defaultdict(list)
    for f in files: csums[f[checksum]].append(f)

    for csum in [cs for cs in csums if len(csums[cs]) > 1]:
        files = csums[csum]
        print(files[0]['size'], 'bytes', checksum, 'checksum', csum)
        for f in csums[csum]: print(os.sep.join(fileson.path(f)))
