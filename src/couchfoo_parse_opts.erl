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

-module(couchfoo_parse_opts).

-export([parse_options/2]).

-include("couchfoo.hrl").


usage() ->
    ?cerr("Usage:~n~n", []),
    ?cerr_tab(), ?cerr("~s [options] database_file~n", [escript:script_name()]),
    ?cerr_nl(),
    ?cerr("Available options are:~n", []),
    ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-h, --help"]), ?cerr("Print help and then exit.", []),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-N, --headers COUNT"]),
    ?cerr("The number of headers to extract and report.", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]),
    ?cerr("Defaults to ~p.", [?DEFAULT_HEADER_COUNT]),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-a, --start-offset OFFSET_A"]),
    ?cerr("The starting file offset (in bytes) from which headers will be", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]), ?cerr("searched backwards.", []),
    ?cerr(" Defaults to the file length (EOF).", []),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-b, --end-offset OFFSET_B"]),
    ?cerr("The file offset (in bytes) at which the header search or count operation", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]), ?cerr("will stop at.", []),
    ?cerr(" Defaults to 0 (the beginning of the file).", []),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["--count-headers"]),
    ?cerr("Count the number of headers in the file, in the range between OFFSET_A ", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]), ?cerr("(supplied by --start-offset) and", []),
    ?cerr(" OFFSET_B (supplied by --end-offset).", []),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-C, --copy-header OFFSET_H"]),
    ?cerr("Grab the header at offset OFFSET_H and append a copy of it to the end of", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]), ?cerr("the given database file.", []),
    ?cerr_nl(), ?cerr_nl(),

    ?cerr_tab(), ?cerr("~-30s ", ["-S, --btree-stats"]),
    ?cerr("Report statistics for each BTree pointed by each reported header.", []),
    ?cerr_nl(), ?cerr_tab(), ?cerr("~-30s ", [""]), ?cerr("Warning: this can be very slow for large databases.", []),
    ?cerr_nl(), ?cerr_nl(),

    halt(1).


parse_options([], Acc) ->
    case proplists:get_value(db_file, Acc) of
    undefined ->
        ?cerr("Error: no database file specified.~n", []),
        halt(1);
    DbFile ->
        validate_db_file(DbFile),
        Acc
    end;

parse_options(["-h" | _RestArgs], _Acc) ->
    usage();
parse_options(["--help" | _RestArgs], _Acc) ->
    usage();

parse_options(["--count-headers" | RestArgs], Acc) ->
    parse_options(RestArgs, [count_headers | Acc]);

parse_options(["--start-offset" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{start_offset, Offset} | Acc]);
parse_options(["-a" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{start_offset, Offset} | Acc]);

parse_options(["--end-offset" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{end_offset, Offset} | Acc]);
parse_options(["-e" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{end_offset, Offset} | Acc]);

parse_options(["--headers" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{header_count, Offset} | Acc]);
parse_options(["-N" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{header_count, Offset} | Acc]);

parse_options(["--copy-header" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{copy_header, Offset} | Acc]);
parse_options(["-C" = Opt | Args], Acc) ->
    {Offset, RestArgs} = parse_int_param(Opt, Args),
    parse_options(RestArgs, [{copy_header, Offset} | Acc]);

parse_options(["--btree-stats" | RestArgs], Acc) ->
    parse_options(RestArgs, [btree_stats | Acc]);

parse_options(["-S" | RestArgs], Acc) ->
    parse_options(RestArgs, [btree_stats | Acc]);

parse_options([("-" ++ _) = Opt], _Acc) ->
    ?cerr("Error: invalid option `~s`~n", [Opt]),
    halt(1);

parse_options([DbFile | RestArgs], Acc) ->
    parse_options(RestArgs, [{db_file, DbFile} | Acc]).


parse_int_param(Opt, []) ->
    ?cerr("Error: integer value missing for the ~s parameter.~n", [Opt]),
    halt(1);
parse_int_param(_Opt, [Offset | RestArgs]) ->
    {list_to_integer(Offset), RestArgs}.


validate_db_file(DbFile) ->
    case file:open(DbFile, [read, raw, binary]) of
    {ok, Fd} ->
        file:close(Fd);
    {error, Error} ->
        ?cerr("Error: couldn't open database file `~s`: ~p~n", [DbFile, Error]),
        halt(1)
    end.
