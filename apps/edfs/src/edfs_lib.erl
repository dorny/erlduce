-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    read_file/3,
    split/3,
    iter_write_list/3,
    iter_write_list/4,
    null_writter/1
]).


read_file(Filename, Opts, Fun) ->
    FileType = proplists:get_value(type, Opts, text),
    BlockSize = proplists:get_value(block_size, Opts, 32*1024*1024),
    Compress = proplists:get_value(compress, Opts, none),
    Args = {FileType, BlockSize, Compress},
    Modes = [read, read_ahead, raw, binary],
    case file:open(Filename, Modes) of
        {ok, IoDev} ->
            Res = split(IoDev, Args, Fun),
            file:close(IoDev),
            Res;
        {error, Reason} ->
            {error, {Filename, Reason}}
    end.


split(IoDev, Args={FileType, BlockSize, Compress}, Fun) ->
    case read_part(FileType, IoDev, BlockSize) of
        {ok, {Bytes, IoDev2}} ->
            {_, Data} = erlduce_utils:encode(Bytes, Compress),
            case Fun(Data) of
                ok -> split(IoDev2, Args, Fun);
                NewFun when is_function(NewFun) -> split(IoDev2, Args, NewFun);
                Error -> Error
            end;
        eof ->
            Fun(eof)
    end.

read_part(binary, IoDev, BlockSize) ->
    case file:read(IoDev, BlockSize) of
        {ok, Data} -> {ok, {Data, IoDev}};
        eof -> eof
    end;

read_part(text, IoDev, BlockSize) ->
    case file:read(IoDev, BlockSize) of
        {ok, Data} ->
            case file:read_line(IoDev) of
                {ok, Line} -> {ok, {[Data,Line], IoDev}};
                eof -> {ok, {Data, IoDev}}
            end;

        eof -> eof
    end;

read_part(lines, IoDev, Number) ->
    read_lines(IoDev, Number, []);

read_part(list, [], _Number) ->
    eof;
read_part(list, List, Number) ->
    {ok, lists:split(Number, List)}.


read_lines(IoDev, 0, Acc) ->
    {ok, {Acc, IoDev}};

read_lines(IoDev, Number, Acc) ->
    case file:read_line(IoDev) of
        {ok, Line} ->
            read_lines(IoDev, Number-1, [Line | Acc]);
        eof ->
            case Acc of
                [] -> eof;
                _ -> {ok, {Acc, IoDev}}
            end
    end.


iter_write_list(LenDef, Path, Replicas) ->
    iter_write_list(LenDef, Path, Replicas, fun erlang:term_to_binary/1).
iter_write_list({LimitType,Limit0}, Path, Replicas, Encode) ->
    AccLimit = erlduce_utils:parse_size(Limit0),
    AddLen = p_iter_add_len(LimitType),
    fun({open, TaskIndex}) ->
        File = filename:join(Path,integer_to_list(TaskIndex)),
        {ok, Inode} = edfs:mkfile(File),
        p_iter_write_list(Inode, Replicas, Encode, [], 0, AccLimit, AddLen)
    end.
p_iter_write_list(Inode, Replicas, Encode, Acc, AccLen, AccLimit, AddLen) ->
    fun
        (close) ->
            case AccLen of
                0 -> ok;
                _ ->
                    Bytes = Encode(lists:reverse(Acc)),
                    case edfs:write(Inode, Bytes, Replicas) of
                        ok -> ok;
                        Error -> exit(Error)
                    end
            end;

        (Item) when AccLen<AccLimit ->
            Acc2 = [Item|Acc],
            AccLen2 = AddLen(Item,AccLen),
            p_iter_write_list(Inode, Replicas, Encode, Acc2, AccLen2, AccLimit, AddLen);

        (Item) ->
            Bytes = Encode(lists:reverse(Acc)),
            case edfs:write(Inode, Bytes, Replicas) of
                ok ->
                    Acc2 = [Item],
                    AccLen2 = AddLen(Item,0),
                    p_iter_write_list(Inode, Replicas, Encode, Acc2, AccLen2, AccLimit, AddLen);
                Error -> exit(Error)
            end
    end.
p_iter_add_len(length) ->
    fun(_Item,Acc) -> Acc+1 end;
p_iter_add_len(size) ->
    fun(Item,Acc) -> Acc+erlang:external_size(Item)-2 end.



null_writter(_) -> fun null_writter/1.
