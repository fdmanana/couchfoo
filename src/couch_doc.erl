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

-module(couch_doc).

-export([rev_to_str/1, revs_to_strs/1]).

-include("couchfoo.hrl").


%% All the following functions were copied, or are slight variations, from
%% Apache CouchDB's couch_doc.erl

revid_to_str(RevId) when size(RevId) =:= 16 ->
    ?l2b(couchfoo_util:to_hex(RevId));
revid_to_str(RevId) ->
    RevId.


rev_to_str({Pos, RevId}) ->
    ?l2b([integer_to_list(Pos), "-", revid_to_str(RevId)]).
                    
                    
revs_to_strs([]) ->
    [];
revs_to_strs([{Pos, RevId}| Rest]) ->
    [rev_to_str({Pos, RevId}) | revs_to_strs(Rest)].
