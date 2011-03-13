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

-define(DEFAULT_HEADER_COUNT, 3).

%% copied from Apache CouchDB's couch_db.hrl
-define(LATEST_DISK_VERSION, 5).

%% copied from Apache CouchDB's couch_db.hrl
-record(db_header, {
     disk_version = ?LATEST_DISK_VERSION,
     update_seq = 0,
     unused = 0,
     fulldocinfo_by_id_btree_state = nil,
     docinfo_by_seq_btree_state = nil,
     local_docs_btree_state = nil,
     purge_seq = 0,
     purged_docs = nil,
     security_ptr = nil,
     revs_limit = 1000
}).

-define(jsone(J), couchfoo_util:json_encode(J)).
-define(jsond(J), couchfoo_util:json_decode(J)).

-define(b2l(V), binary_to_list(V)).
-define(l2b(V), list_to_binary(V)).

-define(cerr(F, A), io:format(standard_error, F, A)).
-define(cout(F, A), io:format(F, A)).
