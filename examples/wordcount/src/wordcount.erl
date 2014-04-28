-module(wordcount).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/3
]).



start(RunID, Slaves, Args=[Src, Dest]) ->

    edfs:rm(Dest),
    edfs:mkdir(Dest),

    {ok,Pid} = erlduce_job:start_link(Slaves, [
        {id, {RunID,'wordcount'}},
        {input, fun()-> edfs:input(Src) end},
        {map, fun(Path, Bytes, Write) ->
            DocID = lists:last(filename:split(Path)),
            words(Bytes, fun(Word)-> Write({Word, 1}) end)
        end},
        {combine, fun(_Key, A,B) ->
            A+B
        end},
        {partition, fun(_Key)-> 0 end},
        {output, edfs_lib:iter_write_list(100000, Dest, 1, fun(Data)->
            [ [Word, " ", integer_to_list(Count), "\n"] || {Word, Count} <- Data]
        end)}

    ]),
    erlduce_job:wait(Pid),
    ok.


words(Bin, F) ->
    words_2(Bin, Bin, 0, 0, F).

words_2(Bin, <<C, Rest/binary>>, Pos, Len, F) when
        (C >= $A) and (C =< $Z);
        (C >= $a) and (C =< $z);
        (C >= $0) and (C =< $9);
        C =:= $_ ->
    words_2(Bin, Rest, Pos+1, Len+1, F);

words_2(Bin, <<_, Rest/binary>>, Pos, 0, F) ->
    words_2(Bin, Rest, Pos+1, 0, F);

words_2(Bin, <<_, Rest/binary>>, Pos, Len, F) ->
    F(binary:part(Bin, Pos-Len, Len)),
    words_2(Bin, Rest, Pos+1, 0, F);

words_2(Bin, <<>>, Pos, Len, F) ->
    case Len of
        0 -> ok;
        _ -> F(binary:part(Bin, Pos-Len, Len)), ok
    end.
