%% -------------------------------------------------------------------
%%
%% gen_term - generate terms
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
%% Generate a bunch of terms suitable for indexing with search,
%% batches 10,000 terms per search (set by ?TERMS_PER_FILE) and
%% generates terms as a, b, ..z, ba, bb, bc, ... baa, bb, bc etc

-module(gen_terms).
-compile([export_all]).

-define(TERMS_PER_FILE, 10000).

%% For running with escript
main([]) ->
    generator(25);
main([Nstr]) ->
    generator(list_to_integer(Nstr)).

%% Generate term files in the current directory for N terms
generator(N) ->
    generator(N, ?TERMS_PER_FILE, undefined).   

generator(0, _FileLeft, undefined) ->
    ok;
generator(0, _FileLeft, Fh) ->
    ok = file:close(Fh);
generator(TotalLeft, 0, Fh) ->
    ok = file:close(Fh),
    generator(TotalLeft, ?TERMS_PER_FILE, undefined);
generator(TotalLeft, FileLeft, undefined) ->
    Fn = make_term(TotalLeft),
    {ok, Fh} = file:open(Fn, [write]),
    generator(TotalLeft, FileLeft, Fh);
generator(TotalLeft, FileLeft, Fh) ->
    Term = make_term(TotalLeft),
    file:write(Fh, [32 | Term]), % <space>Term
    generator(TotalLeft-1, FileLeft-1, Fh).
    
make_term(0) ->
    "a";
make_term(I) ->
    make_term(I, "").

make_term(0, Acc) ->
    Acc;
make_term(I, Acc) ->
    Q = I div 26,
    R = I rem 26,
    make_term(Q, [$a+R | Acc]).
 
