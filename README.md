# Fileson - JSON File database tools

Fileson is a set of Python scripts to create JSON file databases and
use them to do various things, like compare differences between two
databases.

## Create a fileson database

```console
user@server:~$ python3 fileson_create.py files.json ~/mydir
```

This will create a JSON file `files.json` that contains a directory tree
and all file information (name, modified date, size) for `~/mydir`.
To calculate an SHA1 checksum for the files as well:

```console
user@server:~$ python3 fileson_create.py files.json ~/mydir -c sha1
```

You can add `-p` to make the `.json` file more human-readable. See all
options with `-h`.

Calculating SHA1 checksums is somewhat slow, around 1 GB/s on modern m.2 SSD
and 150 MB/s on a mechanical drive, so you can use `-c sha1fast` to only
include the beginning of the file. It will differentiate most cases quite
well.

If you have a previous fileson database at hand, you can use that as a
base and only calculate checksums for new files:

```console
user@server:~$ python3 fileson_create.py files-20210101.json \
  ~/mydir -c sha1 -b files.json
```

Checksum lookup treats files with identical name, size and modification
timestamp as identical. It will remove duplicates from cache (these should
be rare) but will assume that any new or moved files with identical
fingerprint are unchanged. This should work very well for normal use cases.

## Duplicate detection

Once you have a fileson database ready, you can do fun things like see if
you have any duplicates in your folder (cryptic string before duplicates
identifies the `size-hash` collision):

```console
user@server:~$ python3 fileson_duplicates.py pics.json

4838099-1afc8e06e081b772eadd6a981a83f67077e2ef10
./2009/2009-03-07/DSC_3962-2.NEF
./2009/2009-03-07/DSC_3962.NEF
```

Many folders tend to have a lot of small files common (including empty files),
for example source code with git repositories, and that is OK so you can
use for example `-s 1M` to only show duplicates bigger than 1 MiB.

You can skip database creation and give a directory to the command as well:

```console
user@server:~$ python3 fileson_duplicates.py /mnt/d/SomeFolder -s 1M -c sha1fast
```

## Change detection

Once you have a fileson database or two, you can create a two-way delta with
`fileson_delta.py`. Like the duplicate command, one or both can be a directory
(in the latter case you have to specify checksum method yourself, otherwise
it's deducted from database files):

```console
user@server:~$ python3 fileson_delta.py myfiles-2010.json myfiles-2020.json \
  myfiles-2010-2020.delta
```

The `myfiles-2010-2020.delta` now contains a JSON array specifying differences
in the two databases -- files that exist only in origin, only in target, or
have changed. If you are using a checksum, the command will also look up the
file from the other db/directory using the checksum to detect moved files.
Let's say you move `some.zip` around a bit:

```console
user@server:~$ python3 fileson_create.py files.json ~/mydir -c sha1
user@server:~$ mv ~/mydir/some.zip ~/mydir/subdir/newName.zip
user@server:~$ python3 fileson_delta.py files.json ~/mydir
[
  {
    "type": "origin only",
    "path": [
      "."
    ],
    "origin": {
      "name": "some.zip",
      "size": 4687597,
      "modified_gmt": "2021-02-15 20:43:56",
      "sha1": "ff60d87dd0433f93128458c940c8d82e8d3836e9"
    },
    "target_path": [
      ".",
      "subdir",
      "some.zip"
    ]
  },
  {
    "type": "target only",
    "path": [
      ".",
      "subdir"
    ],
    "target": {
      "name": "some.zip",
      "size": 4687597,
      "modified_gmt": "2021-02-15 20:43:56",
      "sha1": "ff60d87dd0433f93128458c940c8d82e8d3836e9"
    },
    "origin_path": [
      ".",
      "some.zip"
    ]
  }
]
```
