-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    read_file/3,
    split/3,
    iter_write_file/3
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



iter_write_file(Path, Encode, Replicas) ->
    fun
        (open, TaskIndex) ->
            File = [Path,"-",integer_to_list(TaskIndex)],
            {ok, Inode} = edfs:mkfile(File),
            Inode;
        (close, _Inode) ->
            ok;
        (Terms, Inode) ->
            Bytes = Encode(Terms),
            edfs:write(Inode, Bytes, Replicas),
            Inode
    end.
