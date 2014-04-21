-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    % cat
    clear/0,
    cp/3,
    % link
    ls/1,
    register_blob/2,
    rm/2,
    % stat/1
    tag/1,
    write/1,
    write/2
]).


start() ->
    edfs_master:start().


clear() ->
    erlduce_utils:run_at_master(edfs_master, clear, []).


cp(Src, Dest, Opts) when is_binary(Src), is_binary(Dest), is_list(Opts) ->
    erlduce_utils:run_at_master(edfs_master, cp, [Src, Dest, Opts]).


ls(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, ls, [Path]).


register_blob(BlobID, Host) ->
    erlduce_utils:run_at_master(edfs_master, register_blob, [BlobID, Host]).


rm(Path, Recursive) when is_binary(Path), is_boolean(Recursive) ->
    erlduce_utils:run_at_master(edfs_master, rm, [Path, Recursive]).


tag(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, tag, [Path]).


write(Bytes) ->
    {ok, EdfsEnv} = application:get_env(erlduce, edfs),
    Replicas = proplists:get_value(replicas, EdfsEnv, 3),
    write(Bytes, Replicas).
write(Bytes, local) ->
    write(Bytes, erlduce_utils:host(node()));
write(Bytes, ReplicasOrHost) when is_binary(Bytes) ->
    Size = byte_size(Bytes),
    case erlduce_utils:run_at_master(edfs_master, allocate, [Size,ReplicasOrHost]) of
        {ok, {BlobID, Hosts}} ->
            edfs_slave:write(Hosts, BlobID, Bytes);
        false ->
            lager:warning("failed to allocate space for [~p]  ~p bytes",[ReplicasOrHost, Size]),
            {error, allocation_failed}
    end.
