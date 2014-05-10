-module(tf_idf).

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
        "args:   ~p~n~n"
    ]),[RunID, ?MODULE, Slaves, Args]),

    TmpDir = "/tmp/tfidf",
    OutDir1 = filename:join(TmpDir, "stage-1"),
    OutDir2 = filename:join(TmpDir, "stage-2"),

    ok = edfs:rm(Dest),
    edfs:mkdir(TmpDir),
    edfs:mkdir(Dest),

    {ok, Docs} = edfs:ls(Src),
    TotalDocs = length(Docs),

    edfs:rm(OutDir1),
    edfs:mkdir(OutDir1),
    {ok,Pid} = erlduce_job:start_link(Slaves, [
        {id, {RunID,1}},
        {input, fun()-> edfs:input(Src) end},
        {map, fun(Path, Bytes, Write) ->
            DocID = lists:last(filename:split(Path)),
            words(Bytes, fun(Word)-> Write({Word,DocID},1) end)
        end},
        {combine, fun(_Key, A,B) ->
            A+B
        end},
        % {output, edfs_lib:iter_write_list(100000, OutDir1, 1, fun erlang:term_to_binary/1)}
        {output, edfs_lib:iter_write_list({size,"32MB"}, OutDir1, 1)},
        {progress, fun(P) -> io:format("done: ~p%~n",[P]) end }
    ]),
    erlduce_job:wait(Pid),

    edfs:rm(OutDir2),
    edfs:mkdir(OutDir2),
    {ok,Pid2} = erlduce_job:start_link(Slaves, [
        {id, {RunID,2}},
        {input, fun()-> edfs:input(OutDir1) end},
        {map, fun(_Path, Bytes, Write) ->
            List = binary_to_term(Bytes),
            [ Write(DocID, {Word, WordCount}) ||  {{Word,DocID}, WordCount} <- List],
            ok
        end},
        {reduce, fun(DocID, Values, Write0)->
            WordsInDoc = lists:foldl(fun({_Word,WordCount}, Sum)-> WordCount+Sum end, 0, Values),
            lists:foldl(fun({Word, WordCount}, Write)->
                Write({{Word, DocID}, {WordCount, WordsInDoc}})
            end, Write0, Values)
        end},
        {output, edfs_lib:iter_write_list({size,"32MB"}, OutDir2, 1)},
        {progress, fun(P) -> io:format("done: ~p%~n",[P]) end }
    ]),
    erlduce_job:wait(Pid2),

    {ok,Pid3} = erlduce_job:start_link(Slaves, [
        {id, {RunID,3}},
        {input, fun()-> edfs:input(OutDir2) end},
        {map, fun(_Path, Bytes, Write) ->
            List = binary_to_term(Bytes),
            [ Write(Word, {DocID, WordCount, WordsInDoc}) ||  {{Word, DocID}, {WordCount, WordsInDoc}} <- List],
            ok
        end},
        {reduce, fun(Word, Values, Write0)->
            FreqInCorpus = length(Values),
            lists:foldl(fun({DocID, WordCount, WordsInDoc}, Write)->
                Write({{Word,DocID}, tdidf(WordCount, WordsInDoc, TotalDocs, FreqInCorpus)})
            end, Write0, Values)
        end},
        % {output, fun edfs_lib:null_writter/1}
        {output, edfs_lib:iter_write_list({size,"32MB"}, Dest, 1, fun(Data)->
            [ [Word, "\t", DocID,"\t", float_to_list(TfIDF), "\n"] || {{Word, DocID}, TfIDF} <- Data]
        end)},
        {progress, fun(P) -> io:format("done: ~p%~n",[P]) end }
    ]),
    erlduce_job:wait(Pid3),

    ok.


words(Bin, Fun) ->
    words_2(Bin, [], Fun).

words_2(<<C, Rest/binary>>, Acc, Fun) when
        (C >= $A) and (C =< $Z);
        (C >= $a) and (C =< $z);
        (C >= $0) and (C =< $9);
        C =:= $_ ->
    words_2(Rest, [C | Acc], Fun);
words_2(<<_, Rest/binary>>, [], Fun) ->
    words_2(Rest, [], Fun);
words_2(<<>>, [], _Fun) ->
    ok;
words_2(Rest, Acc, Fun) ->
    Word = list_to_binary(lists:reverse(Acc)),
    Fun(Word),
    words_2(Rest, [], Fun).




tdidf(WordCount, WordsInDoc, TotalDocs, FreqInCorpus) ->
    (WordCount/WordsInDoc) * math:log(TotalDocs/FreqInCorpus).
