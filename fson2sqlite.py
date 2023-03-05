import json, sys, sqlite3

if len(sys.argv) < 3:
    print('Usage: fson2sqlite [sqlite.db] [file.fson] ...')
    exit(1)

con = sqlite3.connect(sys.argv[1])#, isolation_level=None)
cur = con.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS scans (
    entry TEXT NOT NULL,
    date_gmt TEXT NOT NULL,
    folder TEXT NOT NULL,
    skip TEXT DEFAULT ''
)''')

cur.execute('CREATE INDEX IF NOT EXISTS scan_entry_idx ON scans(entry, date_gmt)')

# op = 0 for deletion, 1 for modification, 2 for creation
# modified_gmt NULL for deletions
# size NULL for directories
# sha1 only for files
cur.execute('''CREATE TABLE IF NOT EXISTS files (
    scan_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    op INTEGER NOT NULL,
    modified_gmt TEXT DEFAULT NULL,
    size INTEGER DEFAULT NULL,
    sha1 TEXT DEFAULT NULL
)''')

cur.execute('CREATE INDEX IF NOT EXISTS file_idx ON files(scan_id, filename)')

for entry in sys.argv[2:]:
    current = {} # Current state

    with open(entry, 'r', encoding='utf8') as fin:
        inmeta = False
        meta = {}
        for l in fin.readlines():
            t = json.loads(l)
            if t[0][0] == ':':
                if not inmeta: meta = {}
                inmeta = True
                meta[t[0]] = t[1]
            else:
                if inmeta:
                    inmeta = False
                    print(meta)
                    cur.execute('''INSERT INTO scans (entry, date_gmt, folder)
                            VALUES (?, ?, ?)''',
                            (entry, meta.get(':date_gmt:', ''), meta.get(':directory:', '')))
                    scan_id = cur.lastrowid

                fn = t[0]
                op = 1 if fn in current else 2 # 1 modified 2 new
                current[fn] = True
                if fn =='.': pass # skip root creation from "legacy" scans and deletion later on
                elif len(t)==1: # deletion
                    del current[fn]
                    cur.execute('''INSERT INTO files (scan_id, filename, op)
                            VALUES(?, ?, ?)''', (scan_id, t[0], 0))
                elif 'size' in t[1]: # new/modified file
                    cur.execute('''INSERT INTO files (scan_id, filename, modified_gmt, size, sha1, op)
                        VALUES(?, ?, ?, ?, ?, ?)''',
                        (scan_id, t[0], t[1]['modified_gmt'], t[1]['size'], t[1]['sha1'], op))
                else: # new/modified folder
                    cur.execute('''INSERT INTO files (scan_id, filename, modified_gmt, op)
                            VALUES(?, ?, ?, ?)''', (scan_id, t[0], t[1]['modified_gmt'], op))
con.commit()
con.close()
