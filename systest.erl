%% -------------------------------------------------------------------
%%
%% systest.erl - k/v test functions
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
%% Helpers for systest

-module(systest).
-compile([export_all]).

write(Size) ->
    write(1, Size, <<"systest">>, 2, <<>>).

write(Start, End, Bucket, W, Magic) ->
    {ok, C} = riak:local_client(),
    F = fun(N, Acc) ->
                Obj = riak_object:new(Bucket, <<N:32/integer>>, <<N:32/integer>>),
                case C:put(Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

read(Size) ->
    read(1, Size, <<"systest">>, 2).
    
read(Start, End, Bucket, R) ->
    {ok, C} = riak:local_client(),
    F = fun(N, Acc) ->
                case C:get(Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riak_object:get_value(Obj) of
                            <<N:32/integer>> ->
                                Acc;
                            WrongVal ->
                                [{N, {wrong_val, WrongVal}} | Acc]
                        end;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).


listkeys(Size) ->
    listkeys(1, Size, <<"systest">>).

listkeys(Start, End, Bucket) ->
    {ok, C} = riak:local_client(),
    Expect = ordsets:from_list([<<V:32/integer>> || V <- lists:seq(Start, End)]),
    {ok, Got} = C:list_keys(Bucket),
    Got2 = ordsets:from_list(Got),
    ordsets:subtract(Expect, Got2) ++ ordsets:subtract(Got2, Expect).
    
