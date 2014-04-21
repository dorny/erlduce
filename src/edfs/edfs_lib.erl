-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    blobs/2,
    blob_filename/1,
    get_available_space/1,
    parent_path/1,
    children/2,
    read_file/3,
    split/4
]).


blobs(Path, BlobsCount) ->
    [ {Path,Part} || Part <- lists:seq(1, BlobsCount)].


blob_filename({Path,Part}) ->
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    Filename = http_uri:encode( filename:join( binary_to_list(Path), integer_to_list(Part))),
    filename:join([WorkDir, <<"blobs">>, Filename]).


get_available_space(Host) ->
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    Node = erlduce_utils:node(edfs,Host),
    case rpc:call(Node, os, cmd, ["df  '"++WorkDir++"' | sed -n '2p' | awk '{print $4}'"]) of
        {badrpc, Error} ->
            {error, Error};
        Space ->
            case string:to_integer(Space) of
                Err={error, _Error}  -> Err;
                {Avail, _} -> Avail
            end
    end.


parent_path(Path) ->
    PathParts = filename:split(Path),
    case length(PathParts) of
        1 ->
            <<"/">>;
        _ ->
            ParentParts = lists:droplast(PathParts),
            filename:join(ParentParts)
    end.


children(Path,Children) ->
    lists:map(fun
        ({link, Link}) -> Link;
        (Child) -> filename:join(Path, Child)
    end, Children).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Readers
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_file(Filename, Opts, Fun) ->
    BlockSize = proplists:get_value(block_size, Opts, 32*1024*1024),
    FileType = proplists:get_value(type, Opts, text),
    % Encoding = proplists:get_value(encoding, Opts, utf8),
    Modes = [read, read_ahead, raw, binary],
    case file:open(Filename, Modes) of
        {ok, IoDev} ->
            split(IoDev, FileType, BlockSize, Fun);
        {error, Reason} ->
            {error, {file_open, Reason}}
    end.

split(IoDev, FileType, BlockSize, Fun) ->
    case read_part(FileType, IoDev, BlockSize) of
        {ok, {Data, IoDev2}} ->
            Fun2 = case Fun(Data) of
                ok -> Fun;
                NewFun when is_function(NewFun) -> NewFun
            end,
            split(IoDev2, FileType, BlockSize, Fun2);
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
