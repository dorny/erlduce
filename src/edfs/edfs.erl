-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    % cat
    clear/0,
    cp/3,
    % link
    ls/1,
    rm/2,
    % stat/1
    tag/1,
    write/2,
    write_local/1
]).


start() ->
    edfs_master:start().


clear() ->
    erlduce_utils:run_at_master(edfs_master, clear, []).


cp(Src, Dest, Opts) when is_binary(Src), is_binary(Dest), is_list(Opts) ->
    erlduce_utils:run_at_master(edfs_master, cp, [Src, Dest, Opts]).


ls(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, ls, [Path]).


rm(Path, Recursive) when is_binary(Path), is_boolean(Recursive) ->
    erlduce_utils:run_at_master(edfs_master, rm, [Path, Recursive]).


tag(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, tag, [Path]).


write(Bytes, Replicas) when is_binary(Bytes), is_integer(Replicas) ->
    Size = byte_size(Bytes),
    case erlduce_utils:run_at_master(edfs_master, allocate, [Size,Replicas]) of
        {ok, {BlobId, Hosts}} ->
            true;
        false ->
            lager:warning("failed to allocate space for ~p * ~p bytes",[Replicas, Size]),
            false
    end.

write_local(Bytes) when is_binary(Bytes) ->
    Size = byte_size(Bytes),
    Host = erlduce_utils:host(),
    case erlduce_utils:run_at_master(edfs_master, allocate_at, [Size, Host]) of
        {ok, BlobId} ->
            lager:info("~p: ~p",[BlobId, Host]),
            true;
        false ->
            lager:warning("failed to allocate space for ~p bytes at ~p",[Size, Host]),
            false
    end.
