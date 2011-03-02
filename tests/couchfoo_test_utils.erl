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

-module(couchfoo_test_utils).

-include_lib("eunit/include/eunit.hrl").
-include("couchfoo_tests.hrl").

-export([copy_db_file/2]).


copy_db_file(Source, Target) ->
    {ok, F1} = file:open(filename:join([?TEST_DBS_DIR, Source]), [read, raw, binary]),
    {ok, F2} = file:open(filename:join([?TMP_DIR, Target]), [write, raw, binary]),
    ?assertEqual(ok, copy_file(F1, F2)),
    ?assertEqual(ok, file:close(F1)),
    ?assertEqual(ok, file:close(F2)).


copy_file(Fd1, Fd2) ->
    case file:read(Fd1, 512 * 1024) of
    eof ->
        ok;
    {ok, Data} ->
        ?assertEqual(ok, file:write(Fd2, Data)),
        copy_file(Fd1, Fd2);
    Error ->
        Error
    end.
