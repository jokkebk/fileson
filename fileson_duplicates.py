from collections import defaultdict
import json, argparse, fileson, os

parser = argparse.ArgumentParser(description='Look for duplicates in Fileson DB')
parser.add_argument('dbfile', type=str, help='Database file (.json appended automatically)')
args = parser.parse_args()

with open(args.dbfile, 'r', encoding='utf8') as fp:
    fs = json.load(fp)
    fileson.addParents(fs)
    files = fileson.filelist(fs)

    if not files:
        print('No files.')
        exit(0)

    checksum = next(c for c in ['sha1fast', 'sha1', 'none'] if c in files[0])
    if checksum == 'none':
        print('No checksum!')
        exit(0)

    csums = defaultdict(list)
    for f in files: csums[f[checksum]].append(f)

    for csum in [cs for cs in csums if len(csums[cs]) > 1]:
        print(csum, 'in:')
        for f in csums[csum]:
            print(f['dir']['path'], f['name'], f['size'])
