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
    format/0,
    get_inode/1,
    input/1,
    ls/1,
    mkdir/1,
    mkfile/1,
    mv/2,
    read/1,
    rm/1,
    stat/1,
    write/3
]).


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
p_cat_blob(Blob, IoDev, DeCompress) ->
    case edfs_slave:read(Blob) of
        {ok, Bytes} ->
            Data = erlduce_utils:decode({DeCompress,binary}, Bytes),
            file:write(IoDev, Data),
            ok;
        {error, Reason} ->
            {error, {Blob, Reason}}
    end.


format() ->
    edfs_master:format().


get_inode(Path)->
    edfs_master:get_inode(p_as_binary(Path)).


input(Path) ->
    case get_inode(Path) of
        {ok, Inode} ->
            {ok, p_input(Path, Inode,[])};
        Error ->
            exit(Error)
    end.
p_input(Path, ParentInode,Acc0) ->
    case stat(ParentInode) of
        {ok, {ChildInode, directory}} ->
            case ls(ChildInode) of
                {ok, Files} ->
                    lists:foldl(fun({File,Inode}, Acc)->
                        p_input(filename:join(Path,File), Inode, Acc)
                    end, Acc0, Files);
                Error -> exit(Error)
            end;
        {ok, {ChildInode, regular}} ->
            case blobs(ChildInode) of
                {ok, Blobs} ->
                    lists:foldl(fun({BlobID, Hosts},Acc)->
                        [{BlobID, Path, Hosts} | Acc]
                    end, Acc0, Blobs);
                Error -> exit(Error)
            end;
        Error -> exit(Error)
    end.


ls(Inode) when is_number(Inode)->
    edfs_master:ls(Inode);
ls(Path) ->
    edfs_master:ls(p_as_binary(Path)).


mkdir(Path)->
    edfs_master:mkdir(p_as_binary(Path)).


read(Blob) ->
    edfs_slave:read(Blob).


rm(Path) ->
    edfs_master:rm(p_as_binary(Path)).


stat(Inode) when is_number(Inode) ->
    edfs_master:stat(Inode);
stat(Path) ->
    edfs_master:stat(p_as_binary(Path)).


mkfile(Path)->
    edfs_master:mkfile(p_as_binary(Path)).


mv(Src,Dest) ->
    edfs_master:mv(p_as_binary(Src),p_as_binary(Dest)).


write(Inode, Bytes, Replicas) when is_integer(Inode) ->
    write2(Inode, Bytes, Replicas);
write(Path, Bytes, ReplicasOrHosts) ->
    case get_inode(Path) of
        {ok, Inode} -> write(Inode, Bytes, ReplicasOrHosts);
        Error -> Error
    end.
write2(Inode, Bytes, Replicas) when is_integer(Inode), is_integer(Replicas) ->
    case edfs_master:mkblob(Inode, iolist_size(Bytes), Replicas) of
        {ok, Blob} -> edfs_slave:write(Blob, Bytes);
        Error -> Error
    end.


p_as_binary(X) when is_binary(X) -> X;
p_as_binary(X) when is_list(X) -> iolist_to_binary(X).
