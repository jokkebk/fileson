import json, sys, sqlite3

if len(sys.argv) < 3:
    print('Usage: fson2sqlite [file.fson] [sqlite.db]')
    exit(1)

con = sqlite3.connect(sys.argv[2])#, isolation_level=None)
cur = con.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS entries (
    name TEXT NOT NULL UNIQUE
)''')

cur.execute('''CREATE TABLE IF NOT EXISTS scans (
    entry_id INTEGER NOT NULL,
    date_gmt TEXT NOT NULL,
    folder TEXT NOT NULL,
    skip TEXT DEFAULT ''
)''')

cur.execute('CREATE INDEX IF NOT EXISTS scan_entry_idx ON scans(entry_id)')

cur.execute('''CREATE TABLE IF NOT EXISTS files (
    scan_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    modified_gmt TEXT DEFAULT NULL,
    size INTEGER DEFAULT NULL,
    sha1 TEXT DEFAULT NULL
)''')

name = sys.argv[1] #input('Name of entry to add: ')

cur.execute('INSERT INTO entries (name) VALUES (?)', (name,))
entry_id = cur.lastrowid
print('Created entry', entry_id)

with open(sys.argv[1], 'r', encoding='utf8') as fin:
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
                cur.execute('''INSERT INTO scans (entry_id, date_gmt, folder)
                        VALUES (?, ?, ?)''',
                        (entry_id, meta.get(':date_gmt:', ''), meta.get(':directory:', '')))
                scan_id = cur.lastrowid

            if len(t)==1: # deletion
                cur.execute('''INSERT INTO files (scan_id, filename)
                        VALUES(?, ?)''', (scan_id, t[0]))
            elif 'size' in t[1]: # new file
                cur.execute('''INSERT INTO files (scan_id, filename, modified_gmt, size, sha1)
                    VALUES(?, ?, ?, ?, ?)''',
                    (scan_id, t[0], t[1]['modified_gmt'], t[1]['size'], t[1]['sha1']))
            else: # new folder
                cur.execute('''INSERT INTO files (scan_id, filename, modified_gmt)
                        VALUES(?, ?, ?)''', (scan_id, t[0], t[1]['modified_gmt']))
con.commit()
con.close()
