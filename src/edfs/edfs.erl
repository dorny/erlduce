-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    cat/3,
    cp/3,
    link/2,
    ls/1,
    register_blob/2,
    rm/2,
    stat/1,
    tag/1,
    unlink/2,
    write/2,
    write/3
]).


start() ->
    edfs_master:start().


cat(Path, IoDev, Extract) when is_binary(Path) ->
    case erlduce_utils:run_at_master(edfs_master, blobs, [Path]) of
        {ok, Blobs} ->
            [p_cat_blob(Blob,IoDev,Extract) || Blob <- Blobs];
        Err -> Err
    end.
p_cat_blob({BlobID, Spec, Hosts}, IoDev, Extract) ->
    case edfs_slave:read(Hosts, BlobID) of
        {ok, Bytes} ->
            Data = case Extract of
                true -> erlduce_utils:decode(Spec, Bytes);
                false -> Bytes
            end,
            if
                is_binary(Data) -> io:put_chars(IoDev, Data);
                true -> io:write(IoDev, Data)
            end;
        {error, Reason} ->
            {error, {BlobID, Reason}}
    end.


cp(Src, Dest, Opts) when is_binary(Src), is_binary(Dest), is_list(Opts) ->
    erlduce_utils:run_at_master(edfs_master, cp, [Src, Dest, Opts]).


link(Parent, Child) when is_binary(Parent), is_binary(Child) ->
    erlduce_utils:run_at_master(edfs_master, link, [Parent, {link,Child}]).


ls(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, ls, [Path]).


register_blob(BlobID, Host) ->
    erlduce_utils:run_at_master(edfs_master, register_blob, [BlobID, Host]).


rm(Path, Recursive) when is_binary(Path), is_boolean(Recursive) ->
    erlduce_utils:run_at_master(edfs_master, rm, [Path, Recursive]).


stat(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, stat, [Path]).


tag(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, tag, [Path]).


unlink(Parent, Child) when is_binary(Parent), is_binary(Child) ->
    erlduce_utils:run_at_master(edfs_master, unlink, [Parent, {link,Child}]).


write(Path, Data) ->
    {ok, EdfsEnv} = application:get_env(erlduce, edfs),
    Replicas = proplists:get_value(replicas, EdfsEnv, 3),
    write(Path, Data, Replicas).
write(Path, Data, local) ->
    write(Path, Data, erlduce_utils:host(node()));
write(Path, {Spec, Bytes}, ReplicasOrHost) ->
    Size = if
        is_binary(Bytes) -> byte_size(Bytes);
        is_list(Bytes) -> iolist_size(Bytes)
    end,
    case erlduce_utils:run_at_master(edfs_master, allocate, [Path,Size,Spec,ReplicasOrHost]) of
        {ok, {BlobID, Hosts}} ->
            edfs_slave:write(Hosts, BlobID, Bytes);
        false ->
            lager:warning("failed to allocate space for ~p bytes at ~p @ ~p",[Size,Path,ReplicasOrHost]),
            {error, allocation_failed}
    end.
