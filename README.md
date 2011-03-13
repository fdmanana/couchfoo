# couchfoo

couchfoo is a standalone command line tool to analyse Apache CouchDB database files.
It also allows to grab an existing header from a database file and append a copy
of it to the end of that same database file. These two main features make it an useful
tool to help recover corrupted database files and hack on the core database engine.
The output is in JSON, so that it can be consumed and used by other tools.

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

* Generate a nice UI or build another tool to consume the JSON output from couchfoo
  and generate the UI (could be an HeatMap for example)

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
$ ./couchfoo -S -N 4 /mnt/cm/fdmanana/test_dbs/large1kb.couch
{
    "file": "/mnt/cm/fdmanana/test_dbs/large1kb.couch",
    "file_size": 480342114,
    "file_block_count": 117272,
    "file_start_offset": 480342114,
    "file_end_offset": 0,
    "max_headers_to_display": 4,
    "headers": [
        {
            "offset": 480342016,
            "block": 117271,
            "corrupted": true,
            "corruption_reason": "header is truncated"
        }, {
            "offset": 480333824,
            "block": 117269,
            "size": 78,
            "version": 5,
            "update_seq": 341300,
            "unused": 0,
            "id_btree": {
                "offset": 480329121,
                "reduction": {
                    "not_deleted_doc_count": 341299,
                    "deleted_doc_count": 0
                },
                "stats": {
                    "depth": 5,
                    "kp_nodes": 1686,
                    "kv_nodes": 31956
                }
            },
            "seq_btree": {
                "offset": 480331751,
                "reduction": {
                    "doc_info_record_count": 341299
                },
                "stats": {
                    "depth": 4,
                    "kp_nodes": 353,
                    "kv_nodes": 17961
                }
            },
            "local_btree": {
                "offset": 480317540,
                "reduction": {
                    "value": "[]"
                },
                "stats": {
                    "depth": 1,
                    "kp_nodes": 0,
                    "kv_nodes": 1
                }
            },
            "purge_seq": 0,
            "purged_docs": {
                "offset": null
            },
            "security_object": {
                "offset": 480321636,
                "value": {"admins":{"names":[],"roles":["boss","foobar"]},"members":{"names":[],"roles":[]}}
            },
            "revs_limit": 1000
        }, {
            "offset": 480325632,
            "block": 117267,
            "size": 78,
            "version": 5,
            "update_seq": 341299,
            "unused": 0,
            "id_btree": {
                "offset": 480167831,
                "reduction": {
                    "not_deleted_doc_count": 341298,
                    "deleted_doc_count": 0
                },
                "stats": {
                    "depth": 5,
                    "kp_nodes": 1686,
                    "kv_nodes": 31956
                }
            },
            "seq_btree": {
                "offset": 479936435,
                "reduction": {
                    "doc_info_record_count": 341298
                },
                "stats": {
                    "depth": 4,
                    "kp_nodes": 353,
                    "kv_nodes": 17961
                }
            },
            "local_btree": {
                "offset": 480317540,
                "reduction": {
                    "value": "[]"
                },
                "stats": {
                    "depth": 1,
                    "kp_nodes": 0,
                    "kv_nodes": 1
                }
            },
            "purge_seq": 0,
            "purged_docs": {
                "offset": null
            },
            "security_object": {
                "offset": 480321636,
                "value": {"admins":{"names":[],"roles":["boss","foobar"]},"members":{"names":[],"roles":[]}}
            },
            "revs_limit": 1000
        }, {
            "offset": 480321536,
            "block": 117266,
            "corrupted": true,
            "corruption_reason": "MD5 checksum mismatch"
        }
    ],
    "valid_headers": 2,
    "corrupted_headers": 2
}

$
</pre>


<pre>
$ ./couchfoo --count-headers test_dbs/foo.couch 
{
    "file": "test_dbs/foo.couch",
    "file_size": 53311,
    "file_block_count": 14,
    "file_start_offset": 53311,
    "file_end_offset": 0,
    "valid_headers_count": 10,
    "corrupted_headers_count": 2
}

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
