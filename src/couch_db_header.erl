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

-module(couch_db_header).

-export([headerbin_to_tuple/1]).
-export([upgrade_header/1]).

-include("couchfoo.hrl").


upgrade_header(Header) ->
    UpgradedHeader = simple_upgrade_record(Header, #db_header{}),
    case element(2, UpgradedHeader) of
    Old when Old < 4 ->
        throw({
            database_header_version_unsupported,
            <<"CouchDB 0.9 (or older) database headers are not supported">>
        });
    4 ->
        % 0.10 and pre 0.11
        UpgradedHeader#db_header{security_ptr = nil};
    ?LATEST_DISK_VERSION ->
        UpgradedHeader;
    V when V > ?LATEST_DISK_VERSION ->
        throw({
            database_header_version_unsupported,
            list_to_binary("database header version " ++ integer_to_list(V) ++
                           " is not yet unsupported")
        })
    end.


%% simple_upgrade_record/2 copied from Apache CouchDB's couch_db_updater.erl

simple_upgrade_record(Old, New) when tuple_size(Old) < tuple_size(New) ->
    OldSz = tuple_size(Old),
    NewValuesTail =
        lists:sublist(tuple_to_list(New), OldSz + 1, tuple_size(New) - OldSz),
    list_to_tuple(tuple_to_list(Old) ++ NewValuesTail);
simple_upgrade_record(Old, _New) ->
    Old.


headerbin_to_tuple(HeaderBin) ->
    case headerbin_to_term(HeaderBin) of
    {ok, HeaderTerm} ->
        case is_tuple(HeaderTerm) of
        true ->
            case tuple_size(HeaderTerm) > 1 of
            true ->
                case element(1, HeaderTerm) of
                db_header ->
                    try
                        {ok, upgrade_header(HeaderTerm)}
                    catch
                    throw:{database_header_version_unsupported, Reason} ->
                        {corrupted_header, Reason};
                    _:Error ->
                        ErrorBin = iolist_to_binary(io_lib:format("~p", [Error])),
                        {corrupted_header, <<"Header record upgrade failed: ", ErrorBin/binary, ".">>}
                    end;
                Tag ->
                    TagBin = iolist_to_binary(io_lib:format("~p", [Tag])),
                    {corrupted_header,
                        <<"Header tuple tag is not db_header but it's `", TagBin/binary, "'.">>}
                end;
            false ->
                {corrupted_header, <<"Header term is a tuple with size < 2.">>}
            end;
        false ->
            {corrupted_header, <<"Header term is not a tuple.">>}
        end;
    _ ->
        {corrupted_header, <<"Header binary is not a serialized Erlang term.">>}
    end.


headerbin_to_term(HeaderBin) ->
    try
        Term = binary_to_term(HeaderBin),
        {ok, Term}
    catch _:_ ->
        false
    end.
