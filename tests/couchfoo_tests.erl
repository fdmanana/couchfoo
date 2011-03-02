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

-module(couchfoo_tests).

-include_lib("eunit/include/eunit.hrl").
-include("couchfoo_tests.hrl").
-include("couchfoo.hrl").


fold_headers_test() ->
    couchfoo_test_utils:copy_db_file("foo.couch", "foo_copy.couch"),
    DbFileName = filename:join([?TMP_DIR, "foo_copy.couch"]),
    OpenResult = couch_file:open(DbFileName),
    ?assertMatch({ok, {file, _, DbFileName}}, OpenResult),
    {ok, File} = OpenResult,

    FoldFun = fun
        ({ok, HeaderTerm, HeaderBin, HeaderBlock}, _File, Acc) ->
            {ValidHeaders, ValidHeadersBlocks, CorruptedHeadersBlocks} = Acc,
            ?assertEqual(term_to_binary(HeaderTerm), HeaderBin),
            {[HeaderTerm | ValidHeaders], [HeaderBlock | ValidHeadersBlocks],
                CorruptedHeadersBlocks};
        ({corrupted_header, Reason, HeaderBlock}, _File, Acc) ->
            {ValidHeaders, ValidHeadersBlocks, CorruptedHeadersBlocks} = Acc,
            ?assertEqual(<<"Header MD5 checksum mismatch.">>, Reason),
            {ValidHeaders, ValidHeadersBlocks, [HeaderBlock | CorruptedHeadersBlocks]}
    end,

    FileSize = couch_file:file_size(File),

    {ValidHeaders1, ValidHeadersBlocks1, CorruptedHeadersBlocks1} =
        couchfoo:fold_headers(File, FileSize, 0, 999, FoldFun, {[], [], []}),

    ?assertEqual(10, length(ValidHeaders1)),
    ?assertEqual((lists:seq(0, 9) -- [5]) ++ [12], ValidHeadersBlocks1),
    ?assertEqual([5], CorruptedHeadersBlocks1),

    {ValidHeaders2, ValidHeadersBlocks2, CorruptedHeadersBlocks2} =
        couchfoo:fold_headers(File, FileSize, 3000, 999, FoldFun, {[], [], []}),

    ?assertEqual(9, length(ValidHeaders2)),
    ?assertEqual((lists:seq(1, 9) -- [5]) ++ [12], ValidHeadersBlocks2),
    ?assertEqual([5], CorruptedHeadersBlocks2),

    {ValidHeaders3, ValidHeadersBlocks3, CorruptedHeadersBlocks3} =
        couchfoo:fold_headers(File, 8192, 3000, 999, FoldFun, {[], [], []}),

    ?assertEqual(2, length(ValidHeaders3)),
    ?assertEqual(lists:seq(1, 2), ValidHeadersBlocks3),
    ?assertEqual([], CorruptedHeadersBlocks3),

    {ValidHeaders4, ValidHeadersBlocks4, CorruptedHeadersBlocks4} =
        couchfoo:fold_headers(File, 8192, 8000, 999, FoldFun, {[], [], []}),

    ?assertEqual(1, length(ValidHeaders4)),
    ?assertEqual([2], ValidHeadersBlocks4),
    ?assertEqual([], CorruptedHeadersBlocks4),

    % let's corrupt header at block 3
    {ok, Fd} = file:open(DbFileName, [write, read, raw, binary]),
    ok = file:pwrite(Fd, couch_file:block_to_offset(3) + 4 + 16 + 4, <<"A">>),
    ok = file:close(Fd),

    {ValidHeaders5, ValidHeadersBlocks5, CorruptedHeadersBlocks5} =
        couchfoo:fold_headers(File, FileSize, 0, 999, FoldFun, {[], [], []}),

    ?assertEqual(9, length(ValidHeaders5)),
    ?assertEqual((lists:seq(0, 9) -- [3, 5]) ++ [12], ValidHeadersBlocks5),
    ?assertEqual([3, 5], CorruptedHeadersBlocks5),

    ?assertEqual(ok, couch_file:close(File)).
