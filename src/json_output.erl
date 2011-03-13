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

-module(json_output).

-export([indent/0, unindent/0]).
-export([newline/0, spaces/1]).
-export([obj_start/0, obj_end/0]).
-export([array_start/0, array_end/0, start_array_element/0]).
-export([obj_field/2, obj_field_name/1, obj_field_value/1]).
-export([set_io_device/1]).

-include("couchfoo.hrl").

-define(INDENT_LEVEL, 4).


set_io_device(IoDev) ->
    erlang:put(io_dev, IoDev).


indent() ->
    erlang:put(indent_level, current_indent() + 1).


unindent() ->
    case current_indent() of
    0 ->
        ok;
    N when N > 0 ->
        erlang:put(indent_level, current_indent() - 1)
    end.


newline() ->
    print("~n", []),
    do_indent().

spaces(N) ->
    print("~s", [lists:duplicate(N, $\s)]).


obj_start() ->
    print("{", []),
    indent(),
    print("~n", []),
    erlang:put(start_object, true).


obj_end() ->
    case erlang:erase(start_object) of
    true ->
        ok;
    _ ->
        print("~n", [])
    end,
    unindent(),
    do_indent(),
    print("}", []).


array_start() ->
    print("[", []),
    erlang:put(array_start, true).


array_end() ->
    print("]", []),
    erlang:erase(array_start).


start_array_element() ->
    case erlang:erase(array_start) of
    true ->
        ok;
    _ ->
        print(", ", [])
    end.


obj_field(Name, Value) ->
    obj_field_name(Name),
    obj_field_value(Value).


obj_field_name(Name) ->
    case erlang:erase(start_object) of
    true ->
        ok;
    _ ->
        print(",~n", [])
    end,
    do_indent(),
    print("~s: ", [ [$", quote_escape(Name), $"] ]).


obj_field_value(nil) ->
    print("null", []);

obj_field_value(true) ->
    print("true", []);

obj_field_value(false) ->
    print("false", []);

obj_field_value(Value) when is_number(Value) ->
    print("~s", [integer_to_list(Value)]);

obj_field_value(Value) when is_binary(Value) orelse is_list(Value) ->
    print("~s", [ [$", quote_escape(Value), $"] ]);

obj_field_value({json, Json}) ->
    print("~s", [Json]);

obj_field_value(Value) ->
    obj_field_value(io_lib:format("~p", [Value])).


current_indent() ->
    case erlang:get(indent_level) of
    undefined ->
        0;
    N when is_integer(N) ->
        N
    end.


do_indent() ->
    print("~s", [lists:duplicate(?INDENT_LEVEL * current_indent(), $\s)]).


print(Fmt, Args) ->
    IoDev = case erlang:get(io_dev) of
    undefined ->
        standard_io;
    Dev ->
        Dev
    end,
    io:format(IoDev, Fmt, Args).


quote_escape(String) ->
    re:replace(String, "\"", "\\\"", [global]).
