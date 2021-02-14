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
you have any duplicates in your folder:

```console
user@server:~$ python3 fileson_duplicates.py pics.json

8244842 bytes sha1 checksum 573a82bac370d800f283c4499289e95cadc354ff :
./2009/2009-03-07/DSC_3962-2.NEF
./2009/2009-03-07/DSC_3962.NEF
```

Many folders tend to have a lot of small files common (including empty files),
for example source code with git repositories, and that is OK so you can
use for example `-s 1M` to only show duplicates bigger than 1 MiB.

## Change detection

Coming up Real Soon(tm).
