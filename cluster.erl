%% -------------------------------------------------------------------
%%
%% cluster.erl - cluster helpers
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
-module(cluster).
-compile([export_all]).

%% Cluster is stable if
%% - all nodes think all other nodes are up
%% - all nodes have the same ring
%% - all nodes only have vnodes running for owned partitions.
is_stable(Node) ->
    %% Get the list of nodes in the cluster
    R = get_ring(Node),
    Nodes = all_members(Node, R),

    %% Ask each node for their rings
    Rings = orddict:from_list([{N,get_ring(N)} || N <- Nodes]),
    
    {N1,R1}=hd(Rings),
    case rings_match(hash_ring(R1), tl(Rings)) of
        {false, N2} ->
            {rings_differ, N1, N2};
        true ->
            %% Work out which vnodes are running and which partitions they claim
            F = fun({N,_R}, Acc) ->
                        {_Pri, Sec, Stopped} = partitions(N),
                        case Sec of
                            [] ->
                                [];
                            _ ->
                                [{waiting_to_handoff, N}]
                        end ++
                        case Stopped of
                            [] ->
                                [];
                            _ ->
                                [{stopped, N}]
                        end ++
                        Acc
                end,
            case lists:foldl(F, [], Rings) of
                [] ->
                    true;
                Issues ->
                    {false, Issues}
            end
    end.

%% Return a list of active primary partitions, active secondary partitions (to be handed off)
%% and stopped partitions that should be started
partitions(Node) ->
    R = get_ring(Node),
    Owners = all_owners(Node, R),
    Owned = ordsets:from_list(owned_partitions(Owners, Node)),
    Active = ordsets:from_list(active_partitions(Node)),
    Stopped = ordsets:subtract(Owned, Active),
    Secondary = ordsets:subtract(Active, Owned),
    Primary = ordsets:subtract(Active, Secondary),
    {Primary, Secondary, Stopped}.

owned_partitions(Owners, Node) ->
    [P || {P, Owner} <- Owners, Owner =:= Node].          

all_owners(Node, R) ->
    rpc:call(Node, riak_core_ring, all_owners, [R]).

all_members(Node, R) ->
    rpc:call(Node, riak_core_ring, all_members, [R]).

get_ring(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    R.

%% Get a list of active partition numbers - regardless of vnode type
active_partitions(Node) ->
    lists:foldl(fun({_,P}, Ps) -> 
                        ordsets:add_element(P, Ps)
                end, [], running_vnodes(Node)).
                            

%% Get a list of running vnodes for a node
running_vnodes(Node) ->
    Pids = vnode_pids(Node),
    [rpc:call(Node, riak_core_vnode, get_mod_index, [Pid]) || Pid <- Pids].
        
%% Get a list of vnode pids for a node
vnode_pids(Node) ->
    [Pid || {_,Pid,_,_} <- supervisor:which_children({riak_core_vnode_sup, Node})].

%% Produce a SHA-1 of the 'chash' portion of the ring
hash_ring(R) ->
    crypto:sha(term_to_binary(element(4,R))).

%% Check if all rings match given {N1,P1} and a list of [{N,P}] to check
rings_match(_, []) ->
    true;
rings_match(R1hash, [{N2, R2} | Rest]) ->
    case hash_ring(R2) of
        R1hash ->
            rings_match(R1hash, Rest);
        _ ->
            {false, N2}
    end.
