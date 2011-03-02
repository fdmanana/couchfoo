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

-module(couchfoo_util).

-export([json_encode/1, json_decode/1]).
-export([to_hex/1]).


json_encode(V) ->
    (mochijson2:encoder([{handler, fun json_encode_handler/1}]))(V).


json_encode_handler({L}) when is_list(L) ->
    {struct, L};
json_encode_handler(Bad) ->
    exit({json_encode, {bad_term, Bad}}).


json_decode(V) ->
    try (mochijson2:decoder([{object_hook, fun json_object_hook/1}]))(V)
    catch
    _Type:_Error ->
        throw({invalid_json, V})
    end.

json_object_hook({struct, L}) ->
    {L}.


to_hex([]) ->
    [];
to_hex(Bin) when is_binary(Bin) ->
    to_hex(binary_to_list(Bin));
to_hex([H|T]) ->
    [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.
