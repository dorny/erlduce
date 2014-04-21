-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    % cat
    clear/0,
    cp/3,
    link/2,
    ls/1,
    register_blob/2,
    rm/2,
    % stat/1
    tag/1,
    unlink/2,
    write/2,
    write/3
]).


start() ->
    edfs_master:start().


clear() ->
    erlduce_utils:run_at_master(edfs_master, clear, []).


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


tag(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs_master, tag, [Path]).


unlink(Parent, Child) when is_binary(Parent), is_binary(Child) ->
    erlduce_utils:run_at_master(edfs_master, unlink, [Parent, {link,Child}]).


write(Path, Bytes) ->
    {ok, EdfsEnv} = application:get_env(erlduce, edfs),
    Replicas = proplists:get_value(replicas, EdfsEnv, 3),
    write(Path, Bytes, Replicas).
write(Path, Bytes, local) ->
    write(Path, Bytes, erlduce_utils:host(node()));
write(Path, Bytes, ReplicasOrHost) when is_binary(Bytes) ->
    Size = byte_size(Bytes),
    case erlduce_utils:run_at_master(edfs_master, allocate, [Path,Size,ReplicasOrHost]) of
        {ok, {BlobID, Hosts}} ->
            edfs_slave:write(Hosts, BlobID, Bytes);
        false ->
            lager:warning("failed to allocate space for ~p bytes at ~p @ ~p",[Size,Path,ReplicasOrHost]),
            {error, allocation_failed}
    end.
