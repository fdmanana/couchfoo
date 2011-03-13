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

-import(json_output, [
    obj_start/0,
    obj_end/0,
    obj_field/2,
    obj_field_name/1,
    obj_field_value/1,
    array_start/0,
    array_end/0,
    start_array_element/0,
    newline/0,
    spaces/1,
    indent/0,
    unindent/0
]).

-record(btree_stats, {
    depth = 0,
    kp_nodes = 0,
    kv_nodes = 0
}).


main(Args) ->
    Options = couchfoo_parse_opts:parse_options(Args, []),
    erlang:put(couchfoo_options, Options),
    FileName = proplists:get_value(db_file, Options),
    {ok, File} = couch_file:open(FileName),
    case proplists:get_value(copy_header, Options) of
    undefined ->
        ok;
    HeaderOffset ->
        Result = append_header_command(File, HeaderOffset),
        ok = couch_file:close(File),
        halt(if Result -> 0; true -> 1 end)
    end,
    obj_start(),
    dump_summary(File),
    case lists:member(count_headers, Options) of
    true ->
        dump_header_count(File);
    false ->
        dump_headers(File)
    end,
    obj_end(),
    newline(),
    ok = couch_file:close(File).


dump_summary(File) ->
    Options = erlang:get(couchfoo_options),
    BlockCount = couch_file:block_count(File),
    FileName = proplists:get_value(db_file, Options),
    FileSize = couch_file:file_size(File),
    StartOffset = proplists:get_value(start_offset, Options, FileSize),
    EndOffset = proplists:get_value(end_offset, Options, 0),
    obj_field("file", FileName),
    obj_field("file_size", FileSize),
    obj_field("file_block_count", BlockCount),
    obj_field("file_start_offset", StartOffset),
    obj_field("file_end_offset", EndOffset),
    case lists:member(count_headers, Options) of
    true ->
        ok;
    false ->
        Max = proplists:get_value(header_count, Options, ?DEFAULT_HEADER_COUNT),
        obj_field("max_headers_to_display", Max)
    end.


dump_header_count(File) ->
    Options = erlang:get(couchfoo_options),
    FileSize = couch_file:file_size(File),
    StartOffset = proplists:get_value(start_offset, Options, FileSize),
    EndOffset = proplists:get_value(end_offset, Options, 0),
    {ok, ValidHeaders, CorruptedHeaders} = couch_file:header_count(
        File,
        lists:min([StartOffset, FileSize]),
        EndOffset),
    obj_field("valid_headers_count", ValidHeaders),
    obj_field("corrupted_headers_count", CorruptedHeaders).


append_header_command(File, HeaderOffset) ->
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
                true;
            Error ->
                ?cerr("Error: couldn't append the header to the file: ~p.~n", [Error]),
                false
            end;
        _ ->
            true
        end;
    _ ->
        ?cerr("Error: no valid header found at offset ~p.~n", [HeaderOffset]),
        false
    end.


dump_headers(File) ->
    Options = erlang:get(couchfoo_options),
    StartOffset = proplists:get_value(start_offset, Options, couch_file:file_size(File)),
    EndOffset = proplists:get_value(end_offset, Options, 0),
    Max = proplists:get_value(header_count, Options, ?DEFAULT_HEADER_COUNT),
    obj_field_name("headers"),
    array_start(),
    indent(),
    newline(),
    {Valid, Corrupt} = fold_headers(
        File, StartOffset, EndOffset, Max, fun dump_header_info/3, {0, 0}),
    unindent(),
    newline(),
    array_end(),
    obj_field("valid_headers", Valid),
    obj_field("corrupted_headers", Corrupt).


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


dump_invalid_header_info(_File, Reason, HeaderBlock, {Valid, Corrupt}) ->
    start_array_element(),
    obj_start(),
    obj_field("offset", couch_file:block_to_offset(HeaderBlock)),
    obj_field("block", HeaderBlock),
    obj_field("corrupted", true),
    obj_field("corruption_reason", Reason),
    obj_end(),
    {Valid, Corrupt + 1}.

dump_valid_header_info(File, HeaderTerm, HeaderBin, HeaderBlock, {Valid, Corrupt}) ->
    start_array_element(),
    obj_start(),
    obj_field("offset", couch_file:block_to_offset(HeaderBlock)),
    obj_field("block", HeaderBlock),
    obj_field("size", byte_size(HeaderBin)),
    dump_header_info(File, HeaderTerm),
    obj_end(),
    {Valid + 1, Corrupt}.


dump_header_info(File, Header) ->
    obj_field("version", Header#db_header.disk_version),
    obj_field("update_seq", Header#db_header.update_seq),
    obj_field("unused", Header#db_header.unused),

    obj_field_name("id_btree"),
    obj_start(),
    case Header#db_header.fulldocinfo_by_id_btree_state of
    {IdOffset, {NotDel, Del}} ->
        obj_field("offset", IdOffset),
        obj_field_name("reduction"),
        obj_start(),
        obj_field("not_deleted_doc_count", NotDel),
        obj_field("deleted_doc_count", Del),
        obj_end(),
        validate_btree_root(File, Header#db_header.fulldocinfo_by_id_btree_state);
    nil ->
        obj_field("offset", nil);
    IdBTreeState ->
        obj_field("error", io_lib:format("bad state: ~p", [IdBTreeState]))
    end,
    obj_end(),

    obj_field_name("seq_btree"),
    obj_start(),
    case Header#db_header.docinfo_by_seq_btree_state of
    {SeqOffset, Count} ->
        obj_field("offset", SeqOffset),
        obj_field_name("reduction"),
        obj_start(),
        obj_field("doc_info_record_count", Count),
        obj_end(),
        validate_btree_root(File, Header#db_header.docinfo_by_seq_btree_state);
    nil ->
        obj_field("offset", nil);
    SeqBTreeState ->
        obj_field("error", io_lib:format("bad state: ~p", [SeqBTreeState]))
    end,
    obj_end(),

    obj_field_name("local_btree"),
    obj_start(),
    case Header#db_header.local_docs_btree_state of
    {LocOffset, Red} ->
        obj_field("offset", LocOffset),
        obj_field_name("reduction"),
        obj_start(),
        obj_field("value", io_lib:format("~p", [Red])),
        obj_end(),
        validate_btree_root(File, Header#db_header.local_docs_btree_state);
    nil ->
        obj_field("offset", nil);
    LocalBTreeState ->
        obj_field("error", io_lib:format("bad state: ~p", [LocalBTreeState]))
    end,
    obj_end(),

    obj_field("purge_seq", Header#db_header.purge_seq),

    obj_field_name("purged_docs"),
    obj_start(),
    case Header#db_header.purged_docs of
    PurgedPointer when is_integer(PurgedPointer) ->
        obj_field("offset", PurgedPointer),
        case couch_file:pread_term(File, PurgedPointer) of
        {ok, PurgedDocs} ->
            case json_purged_docs(PurgedDocs) of
            error ->
                obj_field("error",
                          io_lib:format("bad purged docs value: ~p", [PurgedDocs]));
            {ok, JsonPurged} ->
                obj_field("value", {json, JsonPurged})
            end;
        ErrorPurged ->
            obj_field("error",
                      io_lib:format("error reading purged docs value: ~p", [ErrorPurged]))
        end;
    nil ->
        obj_field("offset", nil);
    Purged ->
        obj_field("error", io_lib:format("bad purged docs pointer: ~p", [Purged]))
    end,
    obj_end(),

    obj_field_name("security_object"),
    obj_start(),
    case Header#db_header.security_ptr of
    SecurityPtr when is_integer(SecurityPtr) ->
        obj_field("offset", SecurityPtr),
        case couch_file:pread_term(File, SecurityPtr) of
        {ok, Security} ->
            case json_security(Security) of
            error ->
                obj_field("error",
                          io_lib:format("bad security object value: ~p", [Security]));
            {ok, SecurityJson} ->
                obj_field("value", {json, SecurityJson})
            end;
        ErrorSec ->
            obj_field("error",
                      io_lib:format("error reading security object: ~p", [ErrorSec]))
        end;
    nil ->
        obj_field("offset", nil);
    Security ->
        obj_field("error", io_lib:format("bad security object pointer: ~p", [Security]))
    end,
    obj_end(),

    obj_field("revs_limit", Header#db_header.revs_limit).


json_purged_docs(PurgedDocs) ->
    try
        {ok, iolist_to_binary(?jsone(lists:map(fun({Id, Revs}) ->
            {Id, couch_doc:revs_to_strs(Revs)}
        end, PurgedDocs)))}
    catch _:_ ->
        error
    end.


json_security(Sec) ->
    try
        {ok, iolist_to_binary(?jsone(Sec))}
    catch _:_ ->
        error
    end.


validate_btree_root(File, {Offset, _Reds} = BTreeState) ->
    case couch_file:pread_term(File, Offset) of
    {ok, Root} ->
        case couch_btree:valid_node(Root) of
        true ->
            maybe_report_btree_stats(BTreeState, File);
        false ->
            obj_field("error", io_lib:format("invalid root node: ~p", [Root]))
        end;
    Error ->
        obj_field("error", io_lib:format("error loading node: ~p", [Error]))
    end.


maybe_report_btree_stats(BTreeState, File) ->
    Options = erlang:get(couchfoo_options),
    case lists:member(btree_stats, Options) of
    false ->
        ok;
    true ->
        obj_field_name("stats"),
        obj_start(),
        try
            Stats = analyze_btree(BTreeState, File),
            report_btree_stats(Stats)
        catch
        throw:{btree_error, Error} ->
            obj_field("error", io_lib:format("error analyzing btree: ~s", [Error]))
        end,
        obj_end()
    end.


report_btree_stats(Acc) ->
    #btree_stats{
        kp_nodes = KpNodes,
        kv_nodes = KvNodes,
        depth = Depth
    } = Acc,
    obj_field("depth", Depth),
    obj_field("kp_nodes", KpNodes),
    obj_field("kv_nodes", KvNodes).


analyze_btree(BTreeState, File) ->
    couch_btree:depth_first_traverse(
        BTreeState, File, fun btree_analyst/3, #btree_stats{}).


btree_analyst(map, {kv_node, _KvList}, Acc) ->
    #btree_stats{depth = Depth, kv_nodes = KvNodes} = Acc,
    Acc#btree_stats{depth = Depth + 1, kv_nodes = KvNodes + 1};

btree_analyst(reduce, {kp_node, _KpList}, AccList) ->
    #btree_stats{
        depth = 1 + lists:max([Depth || #btree_stats{depth = Depth} <- AccList]),
        kp_nodes = 1 + lists:sum([KpNodes || #btree_stats{kp_nodes = KpNodes} <- AccList]),
        kv_nodes = lists:sum([KvNodes || #btree_stats{kv_nodes = KvNodes} <- AccList])
    }.
