# Fileson - JSON File database tools

Fileson is a set of Python scripts to create JSON file databases and
use them to do various things, like compare differences between two
databases. There are a few key files:

* `fileson.py` contains `Fileson` class to read, manipulate and write
Fileson databases.
* `fileson_util.py` is a command-line toolkit to create Fileson
databases and do useful things with them (see below)

## Create a Fileson database

```console
user@server:~$ python3 fileson_util.py scan files.fson ~/mydir
```

This will create a JSON file `files.fson` that contains a directory tree
and all file information (name, modified date, size) for `~/mydir`.
To calculate an SHA1 checksum for the files as well:

```console
user@server:~$ python3 fileson_util.py scan files.fson ~/mydir -c sha1
```

You can add `-p` to make the `.fson` file more human-readable. See all
options with `-h`.

Calculating SHA1 checksums is somewhat slow, around 1 GB/s on modern m.2 SSD
and 150 MB/s on a mechanical drive, so you can use `-c sha1fast` to only
include the beginning of the file. It will differentiate most cases quite
well.

Fileson databases are versioned. Once a database exists, repeated call to
`fileson_util.py scan` will update the database, keeping track of the changes.
You can then use this information to view changes between given runs, etc.

Normally SHA1 checksums are carried over if the previous version had a
file with same name, size and modification time. For a stricter version, you
can use `-s` or `--strict` to require full path match. Note that this means
calculating new checksum for all moved files.

## Duplicate detection

Once you have a Fileson database ready, you can do fun things like see if
you have any duplicates in your folder (cryptic string before duplicates
identifies the checksum collision, whether it is based on size or sha1):

```console
user@server:~$ python3 fileson_util.py duplicates pics.fson

1afc8e06e081b772eadd6a981a83f67077e2ef10
2009/2009-03-07/DSC_3962-2.NEF
2009/2009-03-07/DSC_3962.NEF
```

Many folders tend to have a lot of small files common (including empty files),
for example source code with git repositories, and that is OK so you can
use for example `-m 1M` to only show duplicates that have a minimum size of 1 MB.

You can skip database creation and give a directory to the command as well:

```console
user@server:~$ python3 fileson_util.py duplicates /mnt/d/SomeFolder -m 1M -c sha1fast
```

## Change detection

Once you have a Fileson database or two, you can compare them with
`fileson_util.py diff`. Like the duplicate command, one or both can be a directory
(in the latter case you have to specify checksum method yourself, otherwise
it's deducted from database files that need to have a matching checksum type):

```console
user@server:~$ python3 fileson_util.py diff myfiles-2010.fson myfiles-2020.fson \
  myfiles-2010-2020.delta
```

The `myfiles-2010-2020.delta` now contains a JSON array specifying differences
in the two databases -- files that exist only in origin, only in target, or
have changed. If you are using a checksum, the command will also look up the
file from the other db/directory using the checksum to detect moved files.
You can again use `-p` to indent the for human-readability. Omitting the
delta filename will print to standard output.

Let's say you move `some.zip` around a bit (JSON formatted for clarity):

```console
user@server:~$ python3 fileson_util.py scan files.fson ~/mydir -c sha1
user@server:~$ mv ~/mydir/some.zip ~/mydir/subdir/newName.zip
user@server:~$ python3 fileson_util.py diff files.fson ~/mydir -c sha1 -p
[
  {
    "path": "some.zip",
    "target": null,
    "origin": {
      "size": 0,
      "modified_gmt": "2021-02-23 21:57:25",
      "sha1": "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    },
    "target_path": "subdir/newName.zip"
  },
  {
    "path": "subdir/newName.zip",
    "target": {
      "size": 0,
      "modified_gmt": "2021-02-23 21:57:25",
      "sha1": "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    },
    "origin": null,
    "origin_path": "some.zip"
  }
]
```

Doing an incremental backup would involve grabbing the deltas which have
`origin' set to 'null' and don't have an `origin_path` reference. All other
changes can be replicated with simple copy and delete statements (and recreated
using information in the diff).

Loading Fileson databases has special syntax similar to `git` where you can
revert to previous versions with `db.fson~1` to get the previous version or
`db.fson~3` to back down 3 steps. This makes printing out changes after a scan
a breeze. Instead of the `fileson_util.py diff` invocation above, you could
update the db and see what changed:

```console
user@server:~$ python3 fileson_util.py scan files.fson ~/mydir
No checksum specified, using sha1 from DB
user@server:~$ python3 fileson_util.py diff files.fson~1 files.fson -p
[ same output as the above diff ]
```

Note that you did not have to specify `-c sha1` for the commands because that
is detected automatically from the Fileson DB.
