-module(rewritekey).
-compile([export_all]).

write(Size) ->
    rewrite(Size, 1).

rewrite(Size, Reps) ->
    {ok, C} = riak:local_client(),
    [rewrite(1, Size, <<"systest">>, Rep, C, 2) || Rep <- lists:seq(1, Reps)].


rewrite(Start, End, Bucket, Rep, C, W) ->
    F = fun(N, Acc) ->
                case C:get(Bucket, <<N:32/integer>>) of
                    {error, notfound} ->
                        Obj = riak_object:new(Bucket, <<N:32/integer>>, <<Rep:32/integer>>);
                    {ok, Obj0} ->
                        Obj = riak_object:update_value(Obj0, <<Rep:32/integer>>)
                end,
                case C:put(Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

read(Size) ->
    read(Size, 1).
    
read(Size, Rep) ->
    read(1, Size, <<"systest">>, Rep, 2).
    
read(Start, End, Bucket, Rep, R) ->
    {ok, C} = riak:local_client(),
    F = fun(N, Acc) ->
                case C:get(Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riak_object:get_value(Obj) of
                            <<Rep:32/integer>> ->
                                Acc;
                            WrongVal ->
                                [{N, {wrong_val, WrongVal}} | Acc]
                        end;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

