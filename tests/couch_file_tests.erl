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

-module(couch_file_tests).

-include_lib("eunit/include/eunit.hrl").
-include("couchfoo_tests.hrl").
-include("couchfoo.hrl").


basic_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,

    DbFileSize = filelib:file_size(DbFileName),
    ?assertEqual(DbFileSize, couch_file:file_size(File)),
    ?assertEqual(couch_file:offset_to_block(DbFileSize) + 1, couch_file:block_count(File)),

    ?assertEqual(ok, couch_file:close(File)).


header_count_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,

    {ok, ValidCount1, CorruptedCount1} =
        couch_file:header_count(File, couch_file:file_size(File), 0),
    ?assertEqual(10, ValidCount1),
    ?assertEqual(1, CorruptedCount1),

    {ok, ValidCount2, CorruptedCount2} =
        couch_file:header_count(File, couch_file:file_size(File) * 2, 0),
    ?assertEqual(10, ValidCount2),
    ?assertEqual(1, CorruptedCount2),

    {ok, ValidCount3, CorruptedCount3} =
        couch_file:header_count(File, couch_file:file_size(File), 1),
    ?assertEqual(9, ValidCount3),
    ?assertEqual(1, CorruptedCount3),

    {ok, ValidCount4, CorruptedCount4} =
        couch_file:header_count(File, couch_file:file_size(File), 4096),
    ?assertEqual(9, ValidCount4),
    ?assertEqual(1, CorruptedCount4),

    {ok, ValidCount5, CorruptedCount5} = couch_file:header_count(File, 8192, 0),
    ?assertEqual(3, ValidCount5),
    ?assertEqual(0, CorruptedCount5),

    {ok, ValidCount6, CorruptedCount6} = couch_file:header_count(File, 9192, 0),
    ?assertEqual(3, ValidCount6),
    ?assertEqual(0, CorruptedCount6),

    {ok, ValidCount7, CorruptedCount7} = couch_file:header_count(File, 9500, 1001),
    ?assertEqual(2, ValidCount7),
    ?assertEqual(0, CorruptedCount7),

    % let's corrupt header at block 3
    {ok, Fd} = file:open(DbFileName, [write, read, raw, binary]),
    ok = file:pwrite(Fd, couch_file:block_to_offset(3) + 4 + 16 + 4, <<"A">>),
    ok = file:close(Fd),

    {ok, ValidCount8, CorruptedCount8} =
        couch_file:header_count(File, couch_file:file_size(File), 0),
    ?assertEqual(9, ValidCount8),
    ?assertEqual(2, CorruptedCount8),

    ?assertEqual(ok, couch_file:close(File)).


header_find_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,
    BlockCount = couch_file:block_count(File),

    Find1 = couch_file:find_header(File, couch_file:file_size(File), 0),
    ?assertMatch({ok, _, _, _}, Find1),
    ?assertEqual(BlockCount - 1, element(4, Find1)),

    Find2 = couch_file:find_header(
        File, couch_file:file_size(File), couch_file:file_size(File) - 4096),
    ?assertMatch({ok, _, _, _}, Find2),
    ?assertEqual(Find1, Find2),

    Find3 = couch_file:find_header(File, 8190, 0),
    ?assertMatch({ok, _, _, _}, Find3),
    ?assertEqual(1, element(4, Find3)),

    Find4 = couch_file:find_header(File, 4096, 0),
    ?assertMatch({ok, _, _, _}, Find4),
    ?assertEqual(Find3, Find4),

    Find5 = couch_file:find_header(File, 4000, 0),
    ?assertMatch({ok, _, _, _}, Find5),
    ?assertEqual(0, element(4, Find5)),

    % let's corrupt header at block 3
    {ok, Fd} = file:open(DbFileName, [write, read, raw, binary]),
    ok = file:pwrite(Fd, couch_file:block_to_offset(3) + 4 + 16 + 4, <<"A">>),
    ok = file:close(Fd),

    Find6 = couch_file:find_header(File, couch_file:block_to_offset(3), 0),
    ?assertMatch({corrupted_header, _, 3}, Find6),

    ?assertEqual(ok, couch_file:close(File)).


header_open_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,
    BlockCount = couch_file:block_count(File),

    Result1 = couch_file:open_header(File, couch_file:block_to_offset(BlockCount - 1)),
    ?assertMatch({ok, #db_header{}}, Result1),

    Result2 = couch_file:open_header(File, couch_file:block_to_offset(BlockCount - 1) + 1),
    ?assertMatch(invalid_header, Result2),

    Result3 = couch_file:open_header(File, couch_file:block_to_offset(BlockCount - 1) - 1),
    ?assertMatch(invalid_header, Result3),

    Result4 = couch_file:open_header(File, 8192),
    ?assertMatch({ok, #db_header{}}, Result4),

    Result5 = couch_file:open_header(File, 4096),
    ?assertMatch({ok, #db_header{}}, Result5),

    % let's corrupt header at block 3
    {ok, Fd} = file:open(DbFileName, [write, read, raw, binary]),
    ok = file:pwrite(Fd, couch_file:block_to_offset(3) + 4 + 16 + 4, <<"A">>),
    ok = file:close(Fd),

    Result6 = couch_file:open_header(File, couch_file:block_to_offset(3)),
    ?assertMatch({corrupted_header, _}, Result6),

    ?assertEqual(ok, couch_file:close(File)).


read_header_btree_roots_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,
    BlockCount = couch_file:block_count(File),

    Result1 = couch_file:open_header(File, couch_file:block_to_offset(BlockCount - 1)),
    ?assertMatch({ok, #db_header{}}, Result1),

    {ok, Header} = Result1,
    ?assertMatch(#db_header{
        fulldocinfo_by_id_btree_state = {_ByIdOffset, {_NotDel, _Del}},
        docinfo_by_seq_btree_state = {_BySeqOffset, _Count},
        local_docs_btree_state = {_LocalOffset, _}
    }, Header),
    #db_header{
        fulldocinfo_by_id_btree_state = {ByIdOffset, {_NotDel, _Del}},
        docinfo_by_seq_btree_state = {BySeqOffset, _Count},
        local_docs_btree_state = {LocalOffset, _}
    } = Header,

    ByIdRoot = couch_file:pread_term(File, ByIdOffset),
    ?assertMatch({ok, _}, ByIdRoot),

    BySeqRoot = couch_file:pread_term(File, BySeqOffset),
    ?assertMatch({ok, _}, BySeqRoot),

    LocalRoot = couch_file:pread_term(File, LocalOffset),
    ?assertMatch({ok, _}, LocalRoot),

    ?assertEqual(ok, couch_file:close(File)).


append_header_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy_2.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy_2.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,
    BlockCount = couch_file:block_count(File),

    {ok, ValidHeaderCountBefore, CorruptedHeaderCountBefore} =
        couch_file:header_count(File, couch_file:file_size(File), 0),

    OpenHeaderResult = couch_file:open_header(File, couch_file:block_to_offset(BlockCount - 4)),
    ?assertMatch({ok, #db_header{}}, OpenHeaderResult),
    {ok, OriginHeader} = OpenHeaderResult,

    ?assertEqual(ok, couch_file:append_header(File, OriginHeader)),

    {ok, ValidHeaderCountAfter, CorruptedHeaderCountAfter} =
        couch_file:header_count(File, couch_file:file_size(File), 0),
    ?assertEqual(ValidHeaderCountBefore + 1, ValidHeaderCountAfter),
    ?assertEqual(CorruptedHeaderCountBefore, CorruptedHeaderCountAfter),

    NewBlockCount = couch_file:block_count(File),
    ?assertEqual(BlockCount + 1, NewBlockCount),

    FindResult = couch_file:find_header(File, couch_file:file_size(File), 0),
    ?assertMatch({ok, _, _, _}, FindResult),
    ?assertEqual(NewBlockCount - 1, element(4, FindResult)),
    {ok, NewHeaderTerm, NewHeaderBin, _} = FindResult,

    OpenHeaderResult2 = couch_file:open_header(
         File, couch_file:block_to_offset(NewBlockCount - 1)),
    ?assertMatch({ok, #db_header{}}, OpenHeaderResult2),
    {ok, NewHeaderTerm2} = OpenHeaderResult2,

    ?assertEqual(OriginHeader, NewHeaderTerm),
    ?assertEqual(OriginHeader, NewHeaderTerm2),
    ?assertEqual(term_to_binary(OriginHeader), NewHeaderBin),

    ?assertEqual(ok, couch_file:close(File)).
