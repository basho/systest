%% -------------------------------------------------------------------
%%
%% mrh.erl - map/reduce helper
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
-module(mrh).
-compile([export_all]).

%% Objective: Test keyfilters
%% mrh:count_keys({<<"systest">>, [[<<"greater_than">>,<<500:32/integer>>]]}).
%% mrh:count_keys({<<"systest">>, [[<<"greater_than">>,<<500:32/integer>>]]}).

eg() ->
    systest:write(1000),
    {ok, [0]} = mrh:count_keys({<<"systest">>, [[<<"eq">>,<<0:32/integer>>]]}),
    {ok, [1000]} = mrh:count_keys({<<"systest">>, [[<<"neq">>,<<0:32/integer>>]]}),
    {ok, [500]} = mrh:count_keys({<<"systest">>, [[<<"greater_than">>,<<500:32/integer>>]]}),
    {ok, [750]} = mrh:count_keys({<<"systest">>, [[<<"less_than">>,<<751:32/integer>>]]}),
    {ok, [250]} = mrh:count_keys({<<"systest">>, [[<<"and">>,
                                                   [[<<"greater_than">>,<<500:32/integer>>]],
                                                   [[<<"less_than">>,<<751:32/integer>>]]]]}).

count_keys(Input) ->
    {ok, C} = riak:local_client(),
    Map = {modfun, riak_kv_mapreduce, map_identity},
    Reduce = {modfun, ?MODULE, reduce_count_inputs},
    C:mapred(Input, [{map, Map, none, false},
                     {reduce, Reduce, none, true}]).

count_bucket(Bucket) ->
    {ok, C} = riak:local_client(),
    Map = {modfun, riak_kv_mapreduce, map_identity},
    Reduce = {modfun, ?MODULE, reduce_count_inputs},
    C:mapred_bucket(Bucket, [{map, Map, none, false},
                            {reduce, Reduce, none, true}]).


%% Copy/pasted from riak_kv_mapreduce - not in older versions of riak
reduce_count_inputs(Acc) ->
    {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, Acc}.

%% @spec reduce_count_inputs([term()|integer()], term()) -> [integer()]
%% @doc reduce phase function for reduce_count_inputs/1
reduce_count_inputs(Results, _) ->
    [ lists:foldl(fun input_counter_fold/2, 0, Results) ].

%% @spec input_counter_fold(term()|integer(), integer()) -> integer()
input_counter_fold(PrevCount, Acc) when is_integer(PrevCount) ->
    PrevCount+Acc;
input_counter_fold(_, Acc) ->
    1+Acc.

