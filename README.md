NzbFS
=====

Overview
--------
NzbFS is a read-only userspace network filesystem that reads yenc-encoded files from usenet directly.
NZBs that are dropped in the filesystem are processed internally and then deleted.
A new directory is created with the same name as the NZB without the .nzb extension.
The files given by the NZB are listed inside the new folder.
Additionally, NzbFS can read files inside uncompressed rars transparently.
With NzbFS you should be able to play video/music, mount ISOs, and generally do anything you could with a local file, except data is streamed as needed.

Post Processing
---------------
After an NZB has been added to NzbFS, a user-provided post-processing script is called with the path of the newly created NZB directory.
This can be used to automatically organize files using other programs.
Also, by default, RARs are not "extracted" from the NZB until NzbFS is instructed to do so.
This could happen automatically from the post-processing script by doing something like:

    $ setfattr -n user.nzbfs.cmd -v extract "<mountpoint>/$1"

How It Works
------------
Behind the scenes, all information about remote files are stored in the filesystem at another path, the DB root.
NzbFS passes most filesystem calls through to the underlying filesystem, and intercepts calls to NZB-related files.
Those files are stored in this filesystem database as just the information required to fetch them from the remote server. (Message-IDs, group names)
Extended attributes are required for the underlying filesystem to keep track of things like file size.

Network Layer
-------------
NzbFS tries to be smart about reading files from the NNTP server:
Some caching in memory is done to ensure we aren't hitting the server over and over again for the same article.
Typically, you need multiple threads to max out your connection, which NzbFS should use.
Additionally, a *very* rudimentary/hackish readahead system is used to keep data available for client applications.

When reading files, it tries to return data to the client program as early as possible.
For example, if the client program only requested the first 1024 bytes of a file, NzbFS will fetch the first segment.
As packets are received, the data is yenc decoded and returned to the client, even though the entire segment may not have finished downloading.
This helps applications feel more responsive.

Misc
----

    $ cp ~/Downloads/ubuntu.nzb ~/nzb-mnt/
    $ sudo mount -o loop ~/nzb-mnt/ubuntu/ubuntu-12.10-desktop-amd64.iso /mnt
    $ ls /mnt

Yep, that just happened.

Also, try adding a lot of NZB files and running:

    $ df -h
