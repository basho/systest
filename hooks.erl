%% -------------------------------------------------------------------
%%
%% hooks - pre/postcommit hooks for testing
%%
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%%
%% Pre/post commit hooks for testing
%%
-module(hooks).
-compile([export_all]).

precommit_nop(Obj) ->
    Obj.

precommit_identity(Obj) ->
    Values = riak_object:get_values(Obj),
    riak_object:apply_updates(
      riak_object:update_value(Obj, hd(Values))).

precommit_json_identity(Obj) ->
    Values = riak_object:get_values(Obj),
    {struct, TermList} = mochijson2:decode(hd(Values)),
    riak_object:apply_updates(
      riak_object:update_value(Obj, 
                               iolist_to_binary(mochijson2:encode({struct, TermList})))).

precommit_fail(_Obj) ->
    fail.

precommit_failatom(_Obj) ->
    {fail, on_purpose}.

precommit_failstr(_Obj) ->
    {fail, "on purpose"}.

precommit_failbin(_Obj) ->
    {fail, <<"on purpose">>}.

precommit_failkey(Obj) ->
    case riak_object:key(Obj) of
        <<"fail">> ->
            fail;
        _ ->
            Obj
    end.

                          

set_precommit(Bucket, Hook) when is_atom(Hook) ->
    set_precommit(Bucket, atom_to_binary(Hook, latin1));
set_precommit(Bucket, Hook) when is_list(Hook) ->
    set_precommit(Bucket, list_to_binary(Hook));
set_precommit(Bucket, Hook) ->
    {ok,C} = riak:local_client(),
    C:set_bucket(Bucket, 
                 [{precommit, [{struct,[{<<"mod">>,<<"hooks">>},
                                        {<<"fun">>,Hook}]}]}]).

set_postcommit(Bucket, Hook) when is_atom(Hook) ->
    set_postcommit(Bucket, atom_to_binary(Hook, latin1));
set_postcommit(Bucket, Hook) when is_list(Hook) ->
    set_postcommit(Bucket, list_to_binary(Hook));
set_postcommit(Bucket, Hook) ->
    {ok,C} = riak:local_client(),
    C:set_bucket(Bucket, 
                 [{postcommit, [{struct,[{<<"mod">>,<<"hooks">>},
                                        {<<"fun">>,Hook}]}]}]).

set_hooks() ->
    set_precommit(),
    set_postcommit().

set_precommit() ->
    hooks:set_precommit(<<"nop">>,precommit_nop),
    hooks:set_precommit(<<"identity">>,precommit_identity),
    hooks:set_precommit(<<"json_identity">>,precommit_json_identity),
    hooks:set_precommit(<<"failatom">>,precommit_failatom),
    hooks:set_precommit(<<"failstr">>,precommit_failstr),
    hooks:set_precommit(<<"failbin">>,precommit_failbin),
    hooks:set_precommit(<<"failkey">>,precommit_failkey).
    
set_postcommit() ->
    hooks:set_postcommit(<<"postcrash">>,postcommit_crash),
    hooks:set_postcommit(<<"fail">>, postcommit_log).

postcommit_log(Obj) ->
    Bucket = riak_object:bucket(Obj),
    Key = riak_object:key(Obj),
    File = binary_to_list(<<"/tmp/", Bucket/binary, "_", Key/binary>>),
    Str = lists:flatten(io_lib:format("~p\n", [Obj])),
    file:write_file(File, Str).

postcommit_crash(_Obj) ->
    A = 1,
    B = 2,
    A = B.
