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

-module(couch_btree).

-export([valid_node/1]).
-export([depth_first_traverse/4]).

-include("couchfoo.hrl").


valid_node({kp_node, [_ | _]}) ->
    true;
valid_node({kv_node, [_ | _]}) ->
    true;
valid_node(_) ->
    false.


depth_first_traverse(nil, _File, _Fun, Acc) ->
    Acc;
depth_first_traverse({Offset, _Red}, File, Fun, Acc) ->
    case get_node(File, Offset) of
    {kv_node, _KvList} = Node ->
        Fun(map, Node, Acc);
    {kp_node, KpList} = Node ->
        ChildAccs = lists:map(
            fun({_Key, ChildState}) ->
                depth_first_traverse(ChildState, File, Fun, Acc)
            end,
            KpList),
        Fun(reduce, Node, ChildAccs)
    end.


get_node(File, Offset) ->
    try
        {ok, {NodeType, NodeList}} = couch_file:pread_term(File, Offset),
        {NodeType, NodeList}
    catch T:E ->
        throw({btree_error, iolist_to_binary(
            io_lib:format("Error reading BTree node at offset ~p: ~p:~p~n",
                          [Offset, T, E]))})
    end.
