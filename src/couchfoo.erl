% Copyright 2011,  Filipe David Manana  <fdmanana@apache.org>
% Web site:  http://github.com/fdmanana/couchfoo
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couchfoo).

-export([main/1]).
-export([fold_headers/6]).
%% for testing only
-export([analyze_btree/2]).

-include("couchfoo.hrl").

-record(btree_acc, {
    depth = 0,
    kp_nodes = 0,
    kv_nodes = 0
}).


main(Args) ->
    Options = couchfoo_parse_opts:parse_options(Args, []),
    erlang:put(couchfoo_options, Options),
    FileName = proplists:get_value(db_file, Options),
    {ok, File} = couch_file:open(FileName),
    dump_summary(File),
    ?cout("~n", []),
    dump_headers(File),
    ok = couch_file:close(File).


dump_summary(File) ->
    Options = erlang:get(couchfoo_options),
    BlockCount = couch_file:block_count(File),
    FileName = proplists:get_value(db_file, Options),
    FileSize = couch_file:file_size(File),
    ?cout("Database file `~s` has ~p blocks and is ~p bytes long.~n",
          [FileName, BlockCount, FileSize]),
    case lists:member(count_headers, Options) of
    true ->
        StartOffset = proplists:get_value(start_offset, Options, FileSize),
        EndOffset = proplists:get_value(end_offset, Options, 0),
        {ok, ValidHeaders, CorruptedHeaders} = couch_file:header_count(
            File,
            lists:min([StartOffset, FileSize]),
            EndOffset),
        case (StartOffset =/= FileSize) orelse (EndOffset =/= 0) of
        true ->
            ?cout("~n~p valid headers and ~p corrupted headers found in the file offset "
                  "range [~p, ~p].~n",
                  [ValidHeaders, CorruptedHeaders, StartOffset, EndOffset]);
        false ->
            ?cout("~nFound ~p valid headers and ~p corrupted headers.~n",
                  [ValidHeaders, CorruptedHeaders])
        end,
        halt(0);
    false ->
        maybe_append_header(File)
    end.


maybe_append_header(File) ->
    Options = erlang:get(couchfoo_options),
    case proplists:get_value(copy_header, Options) of
    undefined ->
        ok;
    HeaderOffset ->
        case couch_file:open_header(File, HeaderOffset) of
        {ok, Header} ->
            ?cout("~nAre you sure you to copy the following header to the end of "
                  "the file?~n~n", []),
            dump_header_info(File, Header),
            ?cout("~n", []),
            case string:to_lower(io:get_chars("Y/N ", 1)) of
            "y" ->
                case couch_file:append_header(File, Header) of
                ok ->
                    ?cout("A copy of the header at offset ~p was successfully appended"
                          " to the file.~n~nThe header is", [HeaderOffset]),
                    halt(0);
                Error ->
                    ?cerr("Error: couldn't append the header to the file: ~p.~n", [Error]),
                    halt(1)
                end;
            _ ->
                halt(0)
            end;
        _ ->
            ?cerr("Error: no valid header found at offset ~p.~n", [HeaderOffset]),
            halt(1)
        end
    end.


dump_headers(File) ->
    Options = erlang:get(couchfoo_options),
    StartOffset = proplists:get_value(start_offset, Options, couch_file:file_size(File)),
    EndOffset = proplists:get_value(end_offset, Options, 0),
    Max = proplists:get_value(header_count, Options, ?DEFAULT_HEADER_COUNT),
    DumpCount = fold_headers(File, StartOffset, EndOffset, Max, fun dump_header_info/3, 0),
    ?cout("~p headers shown.~n", [DumpCount]).


fold_headers(_File, _StartOffset, _EndOffset, 0, _Fun, Acc) ->
    Acc;
fold_headers(_File, StartOffset, EndOffset, _Max, _Fun, Acc) when StartOffset < EndOffset ->
    Acc;
fold_headers(File, StartOffset, EndOffset, Max, Fun, Acc) ->
    case couch_file:find_header(File, StartOffset, EndOffset) of
    {ok, _HeaderTuple, _HeaderBin, HeaderBlock} = Ok ->
        Acc2 = Fun(Ok, File, Acc),
        StartOffset2 = couch_file:block_to_offset(HeaderBlock - 1),
        fold_headers(File, StartOffset2, EndOffset, Max - 1, Fun, Acc2);
    {corrupted_header, _Reason, HeaderBlock} = Corrupted ->
        Acc2 = Fun(Corrupted, File, Acc),
        StartOffset2 = couch_file:block_to_offset(HeaderBlock - 1),
        fold_headers(File, StartOffset2, EndOffset, Max - 1, Fun, Acc2);
    _ ->
        Acc
    end.


dump_header_info({ok, HeaderTerm, HeaderBin, HeaderBlock}, File, Acc) ->
    dump_valid_header_info(File, HeaderTerm, HeaderBin, HeaderBlock, Acc);
dump_header_info({corrupted_header, Reason, HeaderBlock}, File, Acc) ->
    dump_invalid_header_info(File, Reason, HeaderBlock, Acc).


dump_invalid_header_info(_File, Reason, HeaderBlock, Acc) ->
    ?cout("Found invalid header at offset ~p (block ~p), reason: ~s~n",
          [couch_file:block_to_offset(HeaderBlock), HeaderBlock, Reason]),
    ?cout("~n", []),
    Acc.

dump_valid_header_info(File, HeaderTerm, HeaderBin, HeaderBlock, Acc) ->
    ?cout("Found header at offset ~p (block ~p), ~p bytes, details:~n~n",
          [couch_file:block_to_offset(HeaderBlock), HeaderBlock, byte_size(HeaderBin)]),
    dump_header_info(File, HeaderTerm),
    ?cout("~n~n", []),
    Acc + 1.


dump_header_info(File, Header) ->
    ?cout_tab(), ?cout("~-40s: ", ["version"]), ?cout("~p~n", [Header#db_header.disk_version]),
    ?cout_tab(), ?cout("~-40s: ", ["update seq"]), ?cout("~p~n", [Header#db_header.update_seq]),
    ?cout_tab(), ?cout("~-40s: ", ["unused"]), ?cout("~p~n", [Header#db_header.unused]),

    ?cout_tab(), ?cout("~-40s: ", ["by ID BTree root offset"]),
    case Header#db_header.fulldocinfo_by_id_btree_state of
    {IdOffset, {NotDel, Del}} ->
        ?cout("~p~n", [IdOffset]),
        ?cout_tab(), ?cout_tab(),
        ?cout("~-36s: ", ["# not deleted documents"]), ?cout("~p~n", [NotDel]),
        ?cout_tab(), ?cout_tab(),
        ?cout("~-36s: ", ["# deleted documents"]), ?cout("~p~n", [Del]),
        validate_btree_root(File, Header#db_header.fulldocinfo_by_id_btree_state);
    nil ->
        ?cout("nil~n", []);
    IdBTreeState ->
        ?cout("unexpected state format - ~p~n", [IdBTreeState])
    end,

    ?cout_tab(), ?cout("~-40s: ", ["by Seq BTree root offset"]),
    case Header#db_header.docinfo_by_seq_btree_state of
    {SeqOffset, Count} ->
        ?cout("~p~n", [SeqOffset]),
        ?cout_tab(), ?cout_tab(),
        ?cout("~-36s: ", ["# doc_info records"]), ?cout("~p~n", [Count]),
        validate_btree_root(File, Header#db_header.docinfo_by_seq_btree_state);
    nil ->
        ?cout("nil~n", []);
    SeqBTreeState ->
         ?cout("unexpected state format - ~p~n", [SeqBTreeState])
    end,

    ?cout_tab(), ?cout("~-40s: ", ["local docs BTree root offset"]),
    case Header#db_header.local_docs_btree_state of
    {LocOffset, _Red} ->
        ?cout("~p~n", [LocOffset]),
        validate_btree_root(File, Header#db_header.local_docs_btree_state);
    nil ->
        ?cout("nil~n", []);
    LocalBTreeState ->
        ?cout("unexpected state format - ~p~n", [LocalBTreeState])
    end,

    ?cout_tab(), ?cout("~-40s: ", ["purge seq"]),
    ?cout("~p~n", [Header#db_header.purge_seq]),
    ?cout_tab(), ?cout("~-40s: ", ["purge docs offset"]),
    ?cout("~p~n", [Header#db_header.purged_docs]),
    case is_integer(Header#db_header.purged_docs) of
    true ->
        ?cout_tab(), ?cout_tab(),
        case couch_file:pread_term(File, Header#db_header.purged_docs) of
        {ok, PurgedDocs} ->
            ?cout("~-36s: ", ["purged docs"]),
            ?cout("~s~n", [json_purged_docs(PurgedDocs)]);
        ErrorPurged ->
            ?cout("~-36s: ", ["purged docs"]),
            ?cout("failure reading purged docs - ~p~n", [ErrorPurged])
        end;
    false ->
        ok
    end,

    ?cout_tab(), ?cout("~-40s: ", ["_security object offset"]),
    ?cout("~p~n", [Header#db_header.security_ptr]),
    case is_integer(Header#db_header.security_ptr) of
    true ->
        ?cout_tab(), ?cout_tab(),
        case couch_file:pread_term(File, Header#db_header.security_ptr) of
        {ok, Security} ->
            ?cout("~-36s: ", ["security_object"]),
            ?cout("~s~n", [json_security(Security)]);
        ErrorSec ->
            ?cout("~-36s: ", ["security object"]),
            ?cout("failure reading _security object - ~p~n", [ErrorSec])
        end;
    false ->
        ok
    end,

    ?cout_tab(), ?cout("~-40s: ", ["revs limit"]),
    ?cout("~p~n", [Header#db_header.revs_limit]).


json_purged_docs(PurgedDocs) ->
    try
        iolist_to_binary(?jsone(lists:map(fun({Id, Revs}) ->
            {Id, couch_doc:revs_to_strs(Revs)}
        end, PurgedDocs)))
    catch _:_ ->
        io_lib:format("unexpected purged docs format - ~p", PurgedDocs)
    end.


json_security(Sec) ->
    try
        iolist_to_binary(?jsone(Sec))
    catch _:_ ->
        io_lib:format("unexpected _security object format - ~p", Sec)
    end.


validate_btree_root(File, {Offset, _Reds} = BTreeState) ->
    case couch_file:pread_term(File, Offset) of
    {ok, Root} ->
        case couch_btree:valid_node(Root) of
        true ->
            maybe_report_btree_stats(BTreeState, File);
        false ->
            ?cout_tab(), ?cout_tab(),
            ?cout("invalid root node - ~s~n", [io_lib:format("~p", [Root])])
        end;
    Error ->
        ?cout_tab(), ?cout_tab(),
        ?cout("error loading root node - ~s~n", [io_lib:format("~p", [Error])])
    end.


maybe_report_btree_stats(BTreeState, File) ->
    Options = erlang:get(couchfoo_options),
    case lists:member(btree_stats, Options) of
    false ->
        ok;
    true ->
        BTreeStats = try
            analyze_btree(BTreeState, File)
        catch
        throw:{btree_error, Error} ->
            ?cout_tab(), ?cout_tab(),
            ?cout("Error traversing the BTree to build the stats: ~s~n", [Error]),
            nil
        end,
        report_btree_stats(BTreeStats)
    end.


report_btree_stats(nil) ->
    ok;
report_btree_stats(Acc) ->
    #btree_acc{
        kp_nodes = KpNodes,
        kv_nodes = KvNodes,
        depth = Depth
    } = Acc,
    ?cout_tab(), ?cout_tab(),
    ?cout("BTree stats~n", []),
    ?cout_tab(), ?cout_tab(), ?cout_tab(),
    ?cout("~-32s: ", ["depth"]), ?cout("~p~n", [Depth]),
    ?cout_tab(), ?cout_tab(), ?cout_tab(),
    ?cout("~-32s: ", ["# kp_nodes"]), ?cout("~p~n", [KpNodes]),
    ?cout_tab(), ?cout_tab(), ?cout_tab(),
    ?cout("~-32s: ", ["# kv_nodes"]), ?cout("~p~n", [KvNodes]),
    ok.


analyze_btree(BTreeState, File) ->
    couch_btree:depth_first_traverse(
        BTreeState, File, fun btree_analyst/3, #btree_acc{}).


btree_analyst(map, {kv_node, _KvList}, Acc) ->
    #btree_acc{depth = Depth, kv_nodes = KvNodes} = Acc,
    Acc#btree_acc{depth = Depth + 1, kv_nodes = KvNodes + 1};

btree_analyst(reduce, {kp_node, _KpList}, AccList) ->
    #btree_acc{
        depth = 1 + lists:max([Depth || #btree_acc{depth = Depth} <- AccList]),
        kp_nodes = 1 + lists:sum([KpNodes || #btree_acc{kp_nodes = KpNodes} <- AccList]),
        kv_nodes = lists:sum([KvNodes || #btree_acc{kv_nodes = KvNodes} <- AccList])
    }.
