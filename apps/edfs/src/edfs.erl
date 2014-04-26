-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(application).

-export([
    start/2,
    stop/1
]).

-export([
    blobs/1,
    cat/2,
    cat/3,
    cp/3,
    format/0,
    get_inode/1,
    input/1,
    ls/1,
    mkdir/1,
    mkfile/1,
    rm/1,
    stat/1,
    write/3
]).

-include("edfs.hrl").
-include_lib("kernel/include/file.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    edfs_sup:start_link().


stop(_State) ->
    ok.


%% ===================================================================
%% API
%% ===================================================================

blobs(Inode) when is_integer(Inode) ->
    edfs_master:blobs(Inode);
blobs(Path) ->
    edfs_master:blobs( p_as_binary(Path)).

cat(Path,  IoDev) ->
    cat(Path,  IoDev, none).
cat(Path,  IoDev, DeCompress) ->
    case blobs(Path) of
        {ok, Blobs} ->
            [ok=p_cat_blob(Blob,IoDev,DeCompress) || Blob <- Blobs],
            ok;
        Err -> Err
    end.
p_cat_blob({BlobID,Hosts}, IoDev, DeCompress) ->
    case edfs_slave:read(BlobID,Hosts) of
        {ok, Bytes} ->
            Data = erlduce_utils:decode({DeCompress,binary}, Bytes),
            file:write(IoDev, Data),
            ok;
        {error, Reason} ->
            {error, {BlobID, Reason}}
    end.


cp(Src, Dest, Opts) ->
    case filelib:is_file(Src) of
        true -> p_cp(p_as_binary(Src),p_as_binary(Dest), Opts);
        false -> {error, {Src, enoent}}
    end.
p_cp(Src, Dest, Opts) ->
    case stat(Dest) of
        {ok, {_, directory}} ->
            Dest2 = filename:join(Dest, lists:last(filename:split(Src))),
            p_cp2(Src, Dest2, Opts);
        {ok, {_, regular}} ->
            {error, {Dest, eexist}};
        {error, enoent} ->
            p_cp2(Src, Dest, Opts)
    end.
p_cp2(Src, Dest, Opts) ->
    case filelib:is_dir(Src) of
        true -> p_cp_dir(Src,Dest, Opts);
        false -> p_cp_file(Src,Dest, Opts)
    end.
p_cp_dir(Src, Dest, Opts) ->
    case mkdir(Dest) of
        {ok, _Inode} ->
            case file:list_dir(Src) of
                {ok, Filenames} ->
                    Errors = lists:foldl(fun(File, Errors)->
                        case p_cp2(filename:join(Src,File), filename:join(Dest,File), Opts) of
                            ok -> Errors;
                            Error -> [Error | Errors]
                        end
                    end, [], Filenames),
                    case Errors of
                        [] -> ok;
                        _ -> lists:flatten(Errors)
                    end;
                {error, Reason} -> {error, {Src,Reason}}
            end;
        {error,Reason} -> {error, {Dest,Reason}}
    end.
p_cp_file(Src,Dest, Opts) ->
    Replicas = proplists:get_value(replicas, Opts, 3),
    case mkfile(Dest) of
        {ok, Inode} ->
            edfs_lib:read_file(Src, Opts, fun
                (eof) -> ok;
                (Bytes) ->
                    case write(Inode, Bytes, Replicas) of
                        ok -> ok;
                        {error, Reason} -> {error, {Dest,Reason}}
                    end
            end);
        {error,Reason} -> {error, {Dest,Reason}}
    end.



format() ->
    edfs_master:format().


get_inode(Path)->
    edfs_master:get_inode(p_as_binary(Path)).


input(Path) ->
    case p_input(Path) of
        {ok, DeepInput} -> {ok, lists:flatten(DeepInput)};
        Error -> Error
    end.
p_input(Path) ->
    case stat(Path) of
        {ok, {Inode, directory}} ->
            case ls(Inode) of
                {ok, Files} ->
                    Res = lists:foldl(fun({File,_Inode}, Acc)->
                        case p_input(filename:join(Path,File)) of
                            {ok, Input} -> [Input|Acc];
                            _ -> Acc
                        end
                    end, [], Files),
                    {ok, Res};
                Error -> Error
            end;
        {ok, {Inode, regular}} ->
            case blobs(Inode) of
                {ok, Blobs} ->
                    {ok, [{Path, BlobID, Hosts} || {BlobID, Hosts} <- Blobs]};
                Error -> Error
            end;
        Error -> Error
    end.



ls(Inode) when is_number(Inode)->
    edfs_master:ls(Inode);
ls(Path) ->
    edfs_master:ls(p_as_binary(Path)).


mkdir(Path)->
    edfs_master:mkdir(p_as_binary(Path)).


rm(Path) ->
    edfs_master:rm(p_as_binary(Path)).


stat(Path) ->
    edfs_master:stat(p_as_binary(Path)).


mkfile(Path)->
    edfs_master:mkfile(p_as_binary(Path)).


write(Inode, Bytes, Replicas) when is_integer(Inode) ->
    write2(Inode, Bytes, Replicas);
write(Path, Bytes, ReplicasOrHosts) ->
    case get_inode(Path) of
        {ok, Inode} -> write(Inode, Bytes, ReplicasOrHosts);
        Error -> Error
    end.
write2(Inode, Bytes, Replicas) when is_integer(Inode), is_binary(Bytes), is_integer(Replicas) ->
    case edfs_master:mkblob(Inode, iolist_size(Bytes), Replicas) of
        {ok, {BlobID, Hosts}} -> edfs_slave:write(BlobID, Bytes, Hosts);
        Error -> Error
    end.


p_as_binary(X) when is_binary(X) -> X;
p_as_binary(X) when is_list(X) -> iolist_to_binary(X).
