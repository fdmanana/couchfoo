# couchfoo

couchfoo is a standalone command line tool to analyse Apache CouchDB database files.
It also allows to grab an existing header from a database file and append a copy
of it to the end of that same database file. These two main features make it an useful
tool to help recover corrupted database files and hack on the core database engine.

Some of the things it currently does:

* scan a database file (or just the region delimited by a given offset range) for
  valid headers and report meaningful information about them and the state of the
  database when each header is the current header. It also reports which headers
  are corrupted and why they are considered corrupted

* count the number of existing valid headers and corrupted headers in a database
  (or just in a region delimited by a given offset range)

* verifies that each valid header points to valid BTree root offsets

* extract a valid header from a database file, make a copy of it and append it to the
  end of that same database file

* analyze the BTrees pointed by each header and report some useful information about
  them such as: depth, # of kp_nodes and # of kv_nodes

## TODOs

* Output all the information in a JSON format so that it can be used by other tools.
  For example a tool to build a UI graph or an Heat Map

* Add more useful BTree statistics, examples: maximum and minimum number of values per
  kv_node and kp_node, maximum file offset distance between consecutive levels in the
  BTree, etc


# Usage

<pre>
$ ./couchfoo -h
Usage:

    ./couchfoo [options] database_file

Available options are:

    -h, --help                     Print help and then exit.

    -N, --headers COUNT            The number of headers to extract and report.
                                   Defaults to 3.

    -a, --start-offset OFFSET_A    The starting file offset (in bytes) from which headers will be
                                   searched backwards. Defaults to the file length (EOF).

    -b, --end-offset OFFSET_B      The file offset (in bytes) at which the header search or count operation
                                   will stop at. Defaults to 0 (the beginning of the file).

    --count-headers                Count the number of headers in the file, in the range between OFFSET_A 
                                   (supplied by --start-offset) and OFFSET_B (supplied by --end-offset).

    -C, --copy-header OFFSET_H     Grab the header at offset OFFSET_H and append a copy of it to the end of
                                   the given database file.

    -S, --btree-stats              Report statistics for each BTree pointed by each reported header.
                                   Warning: this can be very slow for large databases.

$
</pre>

Examples:

<pre>
$ ./couchfoo -S -N 1 /mnt/cm/fdmanana/test_dbs/large1kb.couch 
Database file `/mnt/cm/fdmanana/test_dbs/large1kb.couch` has 117272 blocks and is 480342114 bytes long.

Found header at offset 480342016 (block 117271), 77 bytes, details:

    version                                 : 5
    update seq                              : 341301
    unused                                  : 0
    by ID BTree root offset                 : 480339898
        # not deleted documents             : 341298
        # deleted documents                 : 0
        BTree stats
            depth                           : 5
            # kp_nodes                      : 1686
            # kv_nodes                      : 31956
    by Seq BTree root offset                : 480336370
        # doc_info records                  : 341298
        BTree stats
            depth                           : 4
            # kp_nodes                      : 353
            # kv_nodes                      : 17961
    local docs BTree root offset            : 480317540
        BTree stats
            depth                           : 1
            # kp_nodes                      : 0
            # kv_nodes                      : 1
    purge seq                               : 1
    purge docs offset                       : 480340016
        purged docs                         : {"docfoo1":["1-967a00dff5e02add41819138abb3284d"]}
    _security object offset                 : 480321636
        security_object                     : {"admins":{"names":[],"roles":["boss","foobar"]},"members":{"names":[],"roles":[]}}
    revs limit                              : 1000


1 headers shown.
$
</pre>


<pre>
$ ./couchfoo --count-headers test_dbs/foo.couch 
Database file `test_dbs/foo.couch` has 13 blocks and is 49241 bytes long.

Found 10 valid headers and 1 corrupted headers.
$
</pre>


# Build and testing

<pre>
$ make
</pre>

and

<pre>
$ make test
</pre>
