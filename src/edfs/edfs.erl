-module(edfs).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    clear/0,
    % cp
    % link
    ls/1,
    rm/2,
    % stat/1
    tag/1,
    tag/2,
    tag/3
]).

%% Internal exports
-export([
    rpc_clear/0,
    % rpc_cp,
    % rpc_link,
    rpc_ls/1,
    rpc_rm/2,
    % rpc_stat/1,
    rpc_tag/3,

    p_check_available_space/0,
    p_slave_get_available_space/0
]).

-include("erlduce.hrl").

-define( MIN_AVAIL_SPACE, 100*1024*1024).


start() ->
    p_init_mnesia(),
    p_init_slaves(),
    ok.

clear() ->
    erlduce_utils:run_at_master(edfs, rpc_clear, []).

ls(Path) when is_binary(Path) ->
    erlduce_utils:run_at_master(edfs, rpc_ls, [Path]).


rm(Path, Recursive) when is_binary(Path), is_boolean(Recursive) ->
    erlduce_utils:run_at_master(edfs, rpc_rm, [Path, Recursive]).


tag(Path) ->
    tag(Path,[],[]).
tag(Path, Props) ->
    tag(Path, Props, []).
tag(Path, Props, Blobs) when is_binary(Path), is_list(Props), is_list(Blobs) ->
    erlduce_utils:run_at_master(edfs, rpc_tag, [Path, Props, Blobs]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  SAFE RPC
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rpc_clear() ->
    {atomic, ok} = mnesia:clear_table(edfs_tag),
    {atomic, ok} = mnesia:clear_table(edfs_blob),
    ok = p_insert_root(),
    ok.


rpc_ls(Path) ->
    mnesia:activity(transaction, fun p_ls/1, [Path]).


rpc_rm(<<"/">>, true) ->
    mnesia:activity(transaction, fun p_rm/3, [<<"/">>, true, false]),
    p_insert_root();
rpc_rm(Path, Recursive) ->
    mnesia:activity(transaction, fun p_rm/3, [Path, Recursive, true]).


rpc_tag(Path, Props, Blobs) ->
    mnesia:activity(transaction, fun p_tag/3, [Path, Props, Blobs]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  INIT
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_init_mnesia() ->
    application:load(mnesia),
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    MnesiaDir = filename:join(WorkDir, "mnesia"),
    application:set_env(mnesia, dir, MnesiaDir),

    Node = node(),
    case mnesia:create_schema([Node]) of
        ok ->
            ok = application:start(mnesia),
            p_init_mnesia_tables();

        {error, {Node, {already_exists,Node}}} ->
            ok = application:start(mnesia);

        {error, Error} ->
            lager:error("Failed to create Mnesia schema: ~p",[Error]),
            init:stop(1)
    end.

p_init_mnesia_tables() ->
    {atomic, ok} = mnesia:create_table(edfs_node, [
        {attributes, record_info(fields, edfs_node)},
        {ram_copies, [node()]},
        {type, set}
    ]),
    {atomic, ok} = mnesia:create_table(edfs_tag, [
        {attributes, record_info(fields, edfs_tag)},
        {disc_copies, [node()]},
        {type, set}
    ]),
    {atomic, ok} = mnesia:create_table(edfs_blob, [
        {attributes, record_info(fields, edfs_blob)},
        {disc_copies, [node()]},
        {type, set}
    ]),
    p_insert_root().

p_insert_root() ->
    ok = mnesia:activity(transaction, fun()-> mnesia:write(#edfs_tag{path=(<<"/">>)}) end).


p_init_slaves() ->
    {ok, SlavesDef} = application:get_env(erlduce, nodes),
    Hosts = [ Host || {Host, _Slots} <- SlavesDef],

    Slaves = erlduce_utils:start_slaves(edfs,Hosts),
    EDFSNodes = [ #edfs_node{host=Host,node=Node} || {Host,Node} <- Slaves],
    mnesia:ets(fun()->
        [ mnesia:dirty_write(Node) || Node <- EDFSNodes]
    end),

    p_check_available_space(),
    timer:apply_interval(5*60*1000, edfs, p_check_available_space, []),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  PRIVATE
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_link(Parent, Child) ->
    case mnesia:wread({edfs_tag, Parent}) of
        [ ParentTag ] ->
            Children = ParentTag#edfs_tag.children,
            mnesia:write(ParentTag#edfs_tag{ children=[Child|Children]}),
            ok;
        [] ->
            {error, parent_not_found}
    end.


p_ls(Path) ->
    case mnesia:select(edfs_tag,[{#edfs_tag{path=Path, children='$1', _='_'}, [], ['$1']}]) of
        [Childs] -> {ok, Childs};
        [] ->  {error, not_found}
    end.


p_rm(Path, Recursive, UnlinkParent) ->
    case mnesia:wread({edfs_tag, Path}) of
        [] ->
            {error, not_found};
        [#edfs_tag{children=Children}] when Children=/=[] andalso Recursive=:=false ->
            {error, tag_has_children};
        [#edfs_tag{children=Children, blobs=Blobs}] ->
            lists:foreach(fun(BlobID) -> p_delete_blob(BlobID) end, Blobs),
            lists:foreach(fun(Child) -> p_rm(Child, Recursive, false) end, Children),
            mnesia:delete({edfs_tag, Path}),
            case UnlinkParent of
                true -> p_unlink( p_parent_path(Path), Path);
                false -> ok
            end
    end.


p_tag(Path, Props, Blobs) ->
    case mnesia:read(edfs_tag, Path) of
        [_] -> {error, already_exists};
        [] ->
            ok = mnesia:write(#edfs_tag{path=Path, props=Props, blobs=Blobs}),
            Parent = p_parent_path(Path),
            case p_link(Parent, Path) of
                {error, Reason} -> mnesia:abort(Reason);
                ok -> ok
            end
    end.


p_unlink(Parent, Child) ->
    case mnesia:wread({edfs_tag, Parent}) of
        [Tag] ->
            Children = lists:delete(Child, Tag#edfs_tag.children),
            mnesia:write(Tag#edfs_tag{ children=Children });
        [] ->
            {error, not_found}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  UTILS
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_parent_path(Path) ->
    PathParts = filename:split(Path),
    case length(PathParts) of
        1 ->
            <<"/">>;
        _ ->
            ParentParts = lists:droplast(PathParts),
            filename:join(ParentParts)
    end.


p_check_available_space() ->
    Slaves = mnesia:ets(fun()-> mnesia:dirty_select(edfs_node,[{'_',[],['$_']}]) end),
    erlduce_utils:pmap(fun(#edfs_node{host=Host, node=Node, space_avail=OldAvail }) ->
        case rpc:call(Node, edfs, p_slave_get_available_space, []) of
            Avail when is_integer(Avail), abs(Avail - OldAvail)>?MIN_AVAIL_SPACE ->
                mnesia:activity(transaction, fun()->
                    case mnesia:wread({edfs_node, Host}) of
                        [Rec] -> mnesia:write(Rec#edfs_node{ space_avail=Avail});
                        _ -> ignore
                    end
                end);
            {badrpc, Error} ->
                lager:warning("Failed to check available disk space on: ~p: ~p ",[Node, Error]);
            _ ->
                ok
        end
    end, Slaves).
p_slave_get_available_space() ->
    erlang:list_to_integer(os:cmd("df --output=avail --block-size=1 . | tail -n 1 | tr -d ' \n'")).


p_delete_blob(BlobID) ->
    todo.
