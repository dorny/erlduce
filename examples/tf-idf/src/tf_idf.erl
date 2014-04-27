-module(tf_idf).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    run/2
]).


run(Nodes, Args) ->

    io:format("nodes: ~p~nargs: ~p~n",[Nodes, Args]),

    % erlduce_job:start_link(Nodes, [
    %     {input, edfs:input("dataset")},
    %     {map, fun(Path, Bytes, Write) ->
    %         DocID = lists:last(filename:split(Path)),
    %         words(Bytes, fun(Word)-> Write({{Word,DocID}, 1}) end),
    %         ok
    %     end},
    %     {combine, fun(_Key, A,B) ->
    %         A+B
    %     end},
    %     {output, edfs_lib:iter_write_file("/tmp/tf-idf/1/part", erlang:term_to_binary/1, 1)}
    % ]),

    % erlduce_job:start_link(Nodes, [
    %     {input, edfs:input("/tmp/tf-idf/1")},
    %     {map, fun(Path, Bytes, Write) ->
    %         List = binary_to_term(Bytes),
    %         [ Write({DocID, {Word, WordCount}}) ||  {{Word,DocID}, WordCount} <- List],
    %         ok
    %     end},
    %     {reduce, fun(DocID, Values, Write)->
    %         WordsInDoc = lists:foldl(fun({_Word,WordCount}, Sum)-> WordCount+Sum end, 0, Values),
    %         [ Write({{Word, DocID}, {WordCount, WordsInDoc}}) ||  {Word, WordCount} <- Values].
    %     end},
    %     {output, edfs_lib:iter_write_file("/tmp/tf-idf/2/part", erlang:term_to_binary/1, 1)}
    % ]),

    % erlduce_job:start_link(Nodes, [
    %     {input, edfs:input("/tmp/tf-idf/2")},
    %     {map, fun(Path, Bytes, Write) ->
    %         List = binary_to_term(Bytes),
    %         [ Write({Word, {DocID, WordCount, WordsInDoc}}) ||  {{Word, DocID}, {WordCount, WordsInDoc}} <- List],
    %         ok
    %     end},
    %     {reduce, fun(Word, Values, Write)->
    %         FreqInCorpus = length(Values),
    %         Docs = [ {DocID, tdidf(WordCount, WordsInDoc, TotalDocs, FreqInCorpus)} || {DocID, WordCount, WordsInDoc}  <- Values],
    %         Sorted = lists:keysort(2, Docs),
    %         Write({Word,Sorted}).
    %     end},
    %     {output, edfs_lib:iter_write_file("/tmp/tf-idf/3/part", erlang:term_to_binary/1, 1)}
    % ]),
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


tdidf(WordCount, WordsInDoc, TotalDocs, FreqInCorpus) ->
    (WordCount/WordsInDoc) * math:log(TotalDocs/FreqInCorpus).
