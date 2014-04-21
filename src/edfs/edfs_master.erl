-module(edfs_master).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    allocate/3,
    % cat
    check_available_space/1,
    cp/3,
    link/2,
    ls/1,
    register_blob/2,
    rm/2,
    stat/1,
    tag/1,
    unlink/2
]).

-include("erlduce.hrl").

-define( MIN_AVAIL_SPACE, 100*1024*1024).


start() ->
    p_init_mnesia(),
    p_init_slaves(),
    ok.
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
    mnesia:activity(transaction, fun()-> ok=mnesia:write(#edfs_tag{path=(<<"/">>)}) end).
p_init_slaves() ->
    {ok, SlavesDef} = application:get_env(erlduce, nodes),
    Hosts = [ Host || {Host, _Slots} <- SlavesDef],

    Slaves = erlduce_utils:start_slaves(edfs,Hosts),
    erlduce_utils:pmap(fun({_,Node})->
        {ok, _Pid} = rpc:call(Node, edfs_sup, start_link, [])
    end,Slaves),

    EDFSNodes = [ #edfs_node{host=Host} || {Host,_} <- Slaves],
    mnesia:ets(fun()->
        [ ok=mnesia:dirty_write(Node) || Node <- EDFSNodes]
    end),

    check_available_space(true),
    timer:apply_interval(5*60*1000, ?MODULE, check_available_space, [false]),
    ok.


allocate(Path, Size, Host) when is_atom(Host)->
    mnesia:activity(transaction, fun() ->
        case mnesia:wread({edfs_node, Host}) of
            [Slave=#edfs_node{space=Space}] when Space > Size+?MIN_AVAIL_SPACE ->
                ok=mnesia:write(Slave#edfs_node{space=Space-Size}),
                {ok, BlobID} = p_append_blob(Path, Size, 1),
                {ok, {BlobID, Host}};
            _ ->
                false
        end
    end);
allocate(Path, Size, Replicas) when is_integer(Replicas)->
    p_allocate(Path, Size, Replicas, true).
p_allocate(Path, Size,Replicas, Reset) ->
    Res = mnesia:activity(transaction, fun()->
        MatchSpec = [{#edfs_node{space='$1', used=false, _='_' }, [{'>', '$1', Size+?MIN_AVAIL_SPACE}],['$_']}],
        case mnesia:select(edfs_node, MatchSpec, Replicas, write) of
            '$end_of_table' ->
                false;
            {Slaves, _} ->
                [ok=mnesia:write(Slave#edfs_node{space=Space-Size, used=true}) || Slave=#edfs_node{space=Space} <- Slaves],
                Hosts = [Host || #edfs_node{host=Host} <- Slaves],
                {ok, Hosts}
        end
    end),
    case Res of
        {ok, Hosts} ->
            {ok, BlobID} = p_append_blob(Path, Size, Replicas),
            {ok, {BlobID, Hosts}};
        false when Reset=:=true ->
            p_mark_all_unused(),
            p_allocate(Path, Size,Replicas, false);
        false ->
            false
    end.
p_append_blob(Path, Size, Replicas) ->
    case mnesia:wread({edfs_tag, Path}) of
        [Tag] ->
            Ord = Tag#edfs_tag.blobs + 1,
            BlobID = edfs_lib:blob_id(Path, Ord),
            ok=mnesia:write(#edfs_blob{id=BlobID, size=Size, replicas=Replicas}),
            ok=mnesia:write(Tag#edfs_tag{blobs=Ord}),
            {ok, BlobID};
        [] ->
            {error, not_found}
    end.


check_available_space(Force) ->
    Slaves = mnesia:ets(fun()-> mnesia:dirty_select(edfs_node,[{'_',[],['$_']}]) end),
    erlduce_utils:pmap(fun(#edfs_node{host=Host, space=OldSpace }) ->
        case edfs_lib:get_available_space(Host) of
            Space when ((Space<OldSpace) orelse Force) ->
                mnesia:activity(transaction, fun()->
                    case mnesia:wread({edfs_node, Host}) of
                        [Slave] when ((Space<Slave#edfs_node.space) orelse Force) ->
                            ok=mnesia:write(Slave#edfs_node{ space=Space});
                        _ ->
                            ok
                    end
                end);
            {error, Error} ->
                lager:warning("Failed to check available disk space on: ~p: ~p ",[Host, Error]);
            _ ->
                ok
        end
    end, Slaves).


cp(Src, Dest, Opts) ->
    case tag(Dest) of
        ok -> p_cp(Src, Dest, Opts);
        {error, already_exists} -> p_cp(Src, Dest, Opts);
        Error -> Error
    end.
p_cp(Src, Dest, Opts) ->
    case filelib:is_dir(Src) of
        true -> p_cp_dir(Src, Dest, Opts);
        false ->
            case filelib:is_regular(Src) of
                true -> p_cp_file(Src, Dest, Opts);
                false -> ok
            end
    end.
p_cp_file(Src, Dest, Opts) ->
    Path = filename:join([Dest, filename:basename(Src)]),
    ok = tag(Path),

    ok.
p_cp_dir(Src, Dest, Opts) ->
    case file:list_dir(Src) of
        {ok, Filenames} ->
            Path = filename:join([Dest, filename:basename(Src)]),
            ok = tag(Path),
            [ cp(
                filename:join([Src,Filename]),
                filename:join([Path,Filename]),
                Opts) || Filename <- Filenames
            ];
        {error, Reason} ->
            {warn, Reason}
    end.


link(Parent, Child) ->
    mnesia:activity(transaction, fun() ->
        case mnesia:wread({edfs_tag, Parent}) of
            [ ParentTag ] ->
                Children = ParentTag#edfs_tag.children,
                case lists:member(Child, Children) of
                    false -> ok=mnesia:write(ParentTag#edfs_tag{ children=[ Child | Children]});
                    true -> {error, already_exists}
                end;
            [] ->
                {error, parent_not_found}
        end
    end).


ls(Path) ->
    mnesia:activity(transaction, fun() ->
        case mnesia:select(edfs_tag,[{#edfs_tag{path=Path, children='$1', _='_'}, [], ['$1']}]) of
            [Chidlren] -> {ok, Chidlren};
            [] ->  {error, not_found}
        end
    end).


register_blob(BlobID, Host) ->
    mnesia:activity(transaction, fun()->
        case mnesia:wread({edfs_blob, BlobID}) of
            [Blob] ->
                Hosts = [Host | Blob#edfs_blob.hosts],
                ok=mnesia:write(Blob#edfs_blob{hosts=Hosts});
            [] ->
                lager:error("registering not stored blob"),
                {error, not_found}
        end
    end).


rm(<<"/">>, true) ->
    mnesia:activity(transaction, fun p_rm/3, [<<"/">>, true, false]),
    p_insert_root();
rm(Path, Recursive) ->
    mnesia:activity(transaction, fun p_rm/3, [Path, Recursive, true]).
p_rm(Path, Recursive, UnlinkParent) ->
    case mnesia:wread({edfs_tag, Path}) of
        [] ->
            {error, not_found};
        [#edfs_tag{children=Children}] when Children=/=[] andalso Recursive=:=false ->
            {error, tag_has_children};
        [#edfs_tag{children=Children, blobs=Blobs}] ->
            [ p_delete_blob(edfs_lib:blob_id(Path, BlobOrd)) || BlobOrd <- lists:seq(1, Blobs) ],
            [ p_rm(Child, Recursive, false) || Child <- edfs_lib:children(Path,Children)],
            mnesia:delete({edfs_tag, Path}),
            case UnlinkParent of
                true -> unlink( edfs_lib:parent_path(Path), Path);
                false -> ok
            end
    end.
p_delete_blob(BlobID) ->
    mnesia:activity(transaction, fun()->
        case mnesia:wread({edfs_blob, BlobID}) of
            [#edfs_blob{hosts=Hosts}] ->
                [ edfs_slave:delete(Host, BlobID) || Host <- Hosts],
                ok=mnesia:delete({edfs_blob, BlobID});
            [] ->
                {error, not_found}
        end
    end).


stat(Path) ->
    mnesia:activity(ets, fun()->
        case mnesia:read({edfs_tag, Path}) of
            [#edfs_tag{children=Children, blobs=BlobsCount}] ->
                BlobIDs = [ edfs_lib:blob_id(Path,Ord) || Ord <- lists:seq(1, BlobsCount)],
                OwnSize = lists:foldl(fun(BlobID, Sum)->
                    case mnesia:read({edfs_blob, BlobID}) of
                        [#edfs_blob{size=BlobSize}] -> Sum+BlobSize;
                        [] -> Sum
                    end
                end, 0, BlobIDs),
                ChildrenStats = [stat(Child) || Child <- edfs_lib:children(Path,Children)],
                TotalSize = lists:foldl(fun
                    ({_C,_B,_S,T},Sum)-> Sum+T;
                    (_,Sum) -> Sum
                end, OwnSize, ChildrenStats),
                {length(Children), BlobsCount, OwnSize, TotalSize};
            [] ->
                {error, not_found}
        end
    end).


tag(Path) ->
    mnesia:activity(transaction, fun()->
        case mnesia:read(edfs_tag, Path) of
            [_] -> {error, already_exists};
            [] ->
                ok = mnesia:write(#edfs_tag{path=Path}),
                Parent = edfs_lib:parent_path(Path),
                case link(Parent, filename:basename(Path)) of
                    {error, Reason} -> mnesia:abort(Reason);
                    ok -> ok
                end
        end
    end).


unlink(Parent, Child) ->
    mnesia:activity(transaction, fun() ->
        case mnesia:wread({edfs_tag, Parent}) of
            [Tag] ->
                Children = lists:delete(Child, Tag#edfs_tag.children),
                ok = mnesia:write(Tag#edfs_tag{ children=Children });
            [] ->
                {error, not_found}
        end
    end).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  UTILS
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_mark_all_unused() ->
    mnesia:activity(transaction, fun()->
        Nodes = mnesia:select(edfs_node,[{'_',[],['$_']}]),
        [ ok=mnesia:write(Node#edfs_node{used=false}) || Node <- Nodes],
        ok
    end).
