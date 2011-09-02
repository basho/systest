-module(idxh).
-compile([export_all]).

new(B, PK, SI, V) ->
    riak_object:new(B, PK, V, dict:from_list([{<<"index">>, SI}])).

fixtures() ->
    fixtures(<<"b">>).
    
fixtures(B) ->
    Ks = [
          {<<"k1">>,[{<<"idx1_bin">>, <<"x">>},{<<"idx2_int">>, 1}]},
          {<<"k2">>,[{<<"idx1_bin">>, <<"x">>},
                     {<<"idx1_bin">>, <<"y">>},{<<"idx2_int">>, 2}]},
          {<<"k3">>,[{<<"idx1_bin">>, <<"x">>},
                     {<<"idx1_bin">>, <<"y">>},
                     {<<"idx1_bin">>, <<"z">>},{<<"idx2_int">>, 2}]},
          {<<"k4">>,[{<<"idx1_bin">>, <<"x">>},
                     {<<"idx1_bin">>, <<"y">>},
                     {<<"idx1_bin">>, <<"z">>},
                     {<<"idx1_bin">>, <<"a">>},{<<"idx2_int">>, 3}]},
          {<<"k5">>,[{<<"idx1_bin">>, <<"x">>},
                     {<<"idx1_bin">>, <<"y">>},
                     {<<"idx1_bin">>, <<"z">>},
                     {<<"idx1_bin">>, <<"a">>},
                     {<<"idx1_bin">>, <<"b">>},{<<"idx2_int">>, 3}]},
          {<<"k6">>,[{<<"idx1_bin">>, <<"x">>},
                     {<<"idx1_bin">>, <<"y">>},
                     {<<"idx1_bin">>, <<"z">>},
                     {<<"idx1_bin">>, <<"a">>},
                     {<<"idx1_bin">>, <<"b">>},
                     {<<"idx1_bin">>, <<"c">>},{<<"idx2_int">>, 3}]}
         ],
    {ok,C} = riak:local_client(),
    [begin
         %C:delete(B, K),
         O = new(B, K, SI, term_to_binary({B,K,SI})),
         ok = C:put(O)
     end || {K, SI} <- Ks].
          
search() ->
    search(<<"b">>, <<"idx1_bin">>, <<"x">>).
    
search(B, SI, SK) ->
    {ok,C} = riak:local_client(),
    io:format("Search ~p / ~p = ~p\n ~p\n",
              [B, SI, SK, C:get_index(B, [{eq, SI, SK}])]).

search(B, SI, SK1, SK2) ->
    {ok,C} = riak:local_client(),
    io:format("Search ~p ~p [~p,~p] =\n ~p\n",
              [B, SI, SK1, SK2, C:get_index(B, [{gte, SI, SK1},{lte, SI, SK2}])]).

searchpbc() ->
    searchpbc(<<"b">>, <<"idx1_bin">>, <<"x">>).


searchpbc(B, SI, SK) ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8081),
    io:format("Search ~p / ~p = ~p\n ~p\n",
              [B, SI, SK, riakc_pb_socket:get_index(Pid, B, SI, SK)]).
