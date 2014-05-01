-module(wordcount).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/3
]).



start(RunID, Slaves, Args=[Src, Dest]) ->

    io:format("~n====== DRIVER STARTED ======~n"),
    io:format( lists:concat([
        "run_id: ~p~n",
        "module: ~p~n",
        "slaves: ~p~n",
        "args:   ~p~n"
    ]),[RunID, ?MODULE, Slaves, Args]),

    edfs:rm(Dest),
    edfs:mkdir(Dest),

    JobID = {RunID,'wordcount'},
    {ok,Pid} = erlduce_job:start_link(Slaves, [
        {id, JobID},
        {input, fun()-> edfs:input(Src) end},
        {map, fun(Path, Bytes, Write) ->
            words(Bytes, Write)
        end},
        {combine, fun(_Key, A,B) ->
            A+B
        end},
        {partition, fun(_Key)-> 0 end},
        {output, edfs_lib:iter_write_list(100000, Dest, 1, fun(Data)->
            [ [Word, " ", integer_to_list(Count), "\n"] || {Word, Count} <- Data]
        end)},
        {progress, fun(P) -> io:format("done: ~p%~n",[P]) end }

    ]),
    {ok, Stats} = erlduce_job:wait(Pid),
    io:format("~n====== JOB DONE ======~n"),
    [ io:format("~p: ~p~n",[Key,Val]) || {Key,Val} <- [{id, JobID}|Stats]],
    ok.


words(Bin, Write) ->
    words_2(Bin, [], Write).

words_2(<<C, Rest/binary>>, Acc, Write) when
        (C >= $A) and (C =< $Z);
        (C >= $a) and (C =< $z);
        (C >= $0) and (C =< $9);
        C =:= $_ ->
    words_2(Rest, [C | Acc], Write);
words_2(<<_, Rest/binary>>, [], Write) ->
    words_2(Rest, [], Write);
words_2(<<>>, [], _Write) ->
    ok;
words_2(Rest, Acc, Write) ->
    Word = list_to_binary(lists:reverse(Acc)),
    F = Write({Word, 1}),
    words_2(Rest, [], F).

