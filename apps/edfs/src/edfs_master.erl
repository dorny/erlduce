-module(edfs_master).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/0,

    blobs/1,
    format/0,
    get_inode/1,
    ls/1,
    mkblob/3,
    mkdir/1,
    mkfile/1,
    mv/2,
    register_blob/2,
    rm/1,
    stat/1,

    node_down/2,
    node_up/1

]).

%% private exports
-export([
    p_task/3,
    p_blobs/1,
    p_get_inode/1,
    p_ls/1,
    p_mkblob/4,
    p_mkdir/1,
    p_mkfile/1,
    p_mv/2,
    p_register_blob/2,
    p_rm/1,
    p_stat/1
]).

%% gen_server.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("edfs.hrl").

-define( EDFS, {?MODULE, p_master_node()}).
-define( RPC(F,A), rpc:call(p_master_node(),?MODULE,F,A)).

-record(state, {
    nodes :: list(node()),
    queue ::list(node())
}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).

stat(Path) when is_binary(Path) orelse is_integer(Path) ->
    ?RPC(p_stat, [Path]).

blobs(Path) when is_binary(Path) orelse is_integer(Path) ->
    ?RPC(p_blobs, [Path]).

format() ->
    gen_server:call(?EDFS, format).

get_inode(Path) when is_binary(Path) ->
    ?RPC(p_get_inode, [Path]).

ls(Path) when is_binary(Path) orelse is_integer(Path) ->
    ?RPC(p_ls,[Path]).

mkblob(Inode, Size, Replicas) when is_integer(Inode), is_integer(Size), is_integer(Replicas) ->
    ?RPC(p_mkblob,[Inode, Size, Replicas, erlduce_utils:host()]).

mkdir(Path) when is_binary(Path) ->
    ?RPC(p_mkdir,[Path]).

mkfile(Path) when is_binary(Path) ->
    ?RPC(p_mkfile, [Path]).

mv(Src,Dest) when is_binary(Src), is_binary(Dest)->
    ?RPC(p_mv, [Src,Dest]).

register_blob(BlobID, Host) ->
    ?RPC(p_register_blob, [BlobID, Host]).

rm(Path) when is_binary(Path) ->
    ?RPC(p_rm,[Path]).


node_down(Node, Reason) ->
   gen_server:cast(?EDFS, {node_down, Node, Reason}).

node_up(Node) ->
    gen_server:cast(?EDFS, {node_up, Node}).

%% ===================================================================
%% GEN_SERVER callbacks
%% ===================================================================

init(_Args) ->
    case mnesia:system_info(use_dir) of
        true -> ok;
        false -> lager:warning("Mnesia is not using specified dir")
    end,
    {ok, Hosts} = application:get_env(edfs, hosts),
    Slaves0 = erlduce_utils:start_slaves(edfs_slave, Hosts,[edfs_slave], self()),
    Slaves = lists:foldl(fun
        ({_, {ok,Node}}, Acc) -> [Node | Acc];
        ({Host, {error,Reason}}, Acc) -> lager:warning("Failed to start wroker at ~p: ~p",[Host,Reason]), Acc
    end, [], Slaves0),
    edfs_slave:disk_check(Slaves),
    {ok, #state{
        nodes = Slaves,
        queue = Slaves
    }}.


handle_call( {get_next_hosts,_N, _H}, _From, State=#state{queue=[]}) ->
    {reply, {error, enospc}, State};
handle_call( {get_next_hosts, N, PrefHost}, _From, State=#state{queue=Nodes0}) ->
    PrefNode = erlduce_utils:node(edfs_slave,PrefHost),
    Nodes = case lists:member(PrefNode, Nodes0) of
        true -> [PrefNode | lists:delete(PrefNode, Nodes0) ];
        false -> Nodes0
    end,
    MaxN = min(N, length(Nodes)),
    {Dest, Rest} = lists:split(MaxN, Nodes),
    Nodes2 = Rest++Dest,
    Hosts = [erlduce_utils:host(Node) || Node <- Dest],
    {reply, {ok, Hosts}, State#state{queue=Nodes2}};

handle_call( format, _From, State=#state{nodes=Nodes}) ->
    {reply, p_format(Nodes), State};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {node_down, Node, Reason}, State=#state{queue=Nodes}) ->
    lager:warning("Node down ~p: ~p",[Node, Reason]),
    Nodes2 = lists:delete(Node, Nodes),
    {noreply, State#state{queue=Nodes2}};

handle_cast( {node_up, Node}, State=#state{queue=Nodes}) ->
    lager:info("Node up: ~p",[Node]),
    Nodes2 = case lists:member(Node, Nodes) of
        true -> Nodes;
        false -> [Node | Nodes]
    end,
    Nodes2 = lists:delete(Node, Nodes),
    {noreply, State#state{queue=Nodes2}};

handle_cast( stop, State) ->
    application:stop(edfs),
    {noreply, State};

handle_cast( _Msg, State) ->
    {noreply, State}.


handle_info( _Info, State) ->
    {noreply, State}.


terminate( _Reason,_State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% PRIVATE
%% ===================================================================

p_format(Nodes) ->
    MnesiaNodes = [node()],
    application:stop(mnesia),
    ok=mnesia:delete_schema(MnesiaNodes),
    edfs_slave:format(Nodes),
    case mnesia:create_schema(MnesiaNodes) of
        ok ->
            ok = application:start(mnesia),
            p_format_tables(MnesiaNodes);
        Error ->
            Error
    end.
p_format_tables(Nodes) ->
    {atomic, ok} = mnesia:create_table(edfs_prop, [
        {attributes, record_info(fields, edfs_prop)},
        {disc_copies, Nodes},
        {type, set}
    ]),
    {atomic, ok} = mnesia:create_table(edfs_rec, [
        {attributes, record_info(fields, edfs_rec)},
        {disc_copies, Nodes},
        {type, set}
    ]),
    {atomic, ok} = mnesia:create_table(edfs_blob, [
        {attributes, record_info(fields, edfs_blob)},
        {disc_copies, Nodes},
        {type, set}
    ]),
    {atomic, ok} = mnesia:create_table(edfs_inodes, [
        {attributes, record_info(fields, edfs_inodes)},
        {disc_copies, Nodes},
        {type, ordered_set}
    ]),
    ok = mnesia:activity(transaction, fun()->
        mnesia:write(#edfs_rec{ inode=0, type=directory, children=gb_trees:empty() }),
        mnesia:write(#edfs_prop{ key=inode, value=0 })
    end).


p_task(F,A,P) ->
    link(P),
    P ! {self(), erlang:apply(?MODULE, F, A)}.


p_blobs(Path) when is_binary(Path) ->
    case p_get_inode(Path) of
        {ok, Inode} -> p_blobs(Inode);
        Error -> Error
    end;
p_blobs(Inode) when is_integer(Inode) ->
    mnesia:activity(ets, fun()->
        case mnesia:read(edfs_rec, Inode) of
            [#edfs_rec{type=regular, children=BlobsCount}] ->
                {ok, [ p_blob(BlobID) || BlobID <- p_make_blob_ids(Inode,BlobsCount)]};
            [_] -> {error, eisdir};
            [] -> {error, enoent}
        end
    end).
p_blob(BlobID) ->
    mnesia:activity(ets, fun()->
        case mnesia:read(edfs_blob,BlobID) of
            [#edfs_blob{hosts=Hosts}] ->
                {BlobID, Hosts};
            _ -> exit({error, missing_blob_record})
        end
    end).



p_get_inode(Path) when is_binary(Path) ->
    p_get_inode(filename:split(Path));
p_get_inode([<<"/">> | Rest]) ->
    p_get_inode2(0, Rest);
p_get_inode(Parts) ->
    p_get_inode2(0, Parts).
p_get_inode2(Inode, []) ->
    {ok, Inode};
p_get_inode2(Inode, [Filename | T]) ->
    mnesia:activity(ets, fun()->
        case mnesia:read(edfs_rec, Inode) of
            [#edfs_rec{type=directory, children=Children}] ->
                case gb_trees:lookup(Filename, Children) of
                    {value, NextInode} -> p_get_inode2(NextInode, T);
                    none -> {error, enoent}
                end;
            _ -> {error, enoent}
        end
    end).

p_get_next_hosts(Replicas, Host) ->
    gen_server:call(?EDFS, {get_next_hosts,Replicas,Host}).


p_ls(Dirpath) when is_binary(Dirpath) ->
    case p_get_inode(Dirpath) of
        {ok, Inode} -> p_ls(Inode);
        Error -> Error
    end;
p_ls(Inode) when is_integer(Inode) ->
    mnesia:activity(ets, fun()->
        case mnesia:read(edfs_rec, Inode) of
            [#edfs_rec{type=directory, children=Children}] ->
                {ok, gb_trees:to_list(Children)};
            [_] -> {error, enotdir};
            [] -> {error, enoent}

        end
    end).


p_mkblob(Inode, Size, Replicas, PrefHost) ->
    case p_get_next_hosts(Replicas, PrefHost) of
        {ok, Hosts} -> p_mkblob2(Inode, Size, Replicas, Hosts);
        Error -> Error
    end.
p_mkblob2(Inode, Size, Replicas, Hosts) ->
    mnesia:activity(transaction, fun()->
        case mnesia:wread({edfs_rec, Inode}) of
            [Rec=#edfs_rec{type=regular, children=BlobsCount}] ->
                BlobId = {Inode, BlobsCount+1},
                mnesia:write(Rec#edfs_rec{children=BlobsCount+1}),
                mnesia:write(#edfs_blob{ id=BlobId, size=Size, replicas=Replicas}),
                {ok, {BlobId, Hosts}};
            [_] ->
                {error,eisdir};
            []  ->
                {error, enoent}
        end
    end).


p_mkdir(Dirpath) ->
    PathParts = filename:split(Dirpath),
    {ParentParts,[Filename]} = lists:split(length(PathParts)-1, PathParts),
    mnesia:activity(transaction, fun()->
        case p_get_inode(ParentParts) of
            {ok, ParentInode} -> p_mkdir(ParentInode,Filename);
            Error -> Error
        end
    end).
p_mkdir(ParentInode, Filename) ->
    case mnesia:read(edfs_rec, ParentInode) of
        [Parent=#edfs_rec{type=directory, children=Children}] ->
            case gb_trees:lookup(Filename, Children) of
                none ->
                    NewInode = p_next_inode(),
                    Children2 = gb_trees:insert(Filename, NewInode, Children),
                    mnesia:write(Parent#edfs_rec{children=Children2}),
                    mnesia:write(#edfs_rec{inode=NewInode, type=directory, children=gb_trees:empty()}),
                    {ok, NewInode};
                {value, _Inode} ->
                    {error, eexist}
            end;
        _ -> {error, enoent}
    end.


p_mkfile(Filepath) ->
    PathParts = filename:split(Filepath),
    {ParentParts,[Filename]} = lists:split(length(PathParts)-1, PathParts),
    mnesia:activity(transaction, fun()->
        case p_get_inode(ParentParts) of
            {ok, ParentInode} -> p_mkfile(ParentInode, Filename);
            Error -> Error
        end
    end).
p_mkfile(ParentInode, Filename) ->
    case mnesia:read(edfs_rec, ParentInode) of
        [Parent=#edfs_rec{type=directory, children=Children}] ->
            case gb_trees:is_defined(Filename, Children) of
                false ->
                    NewInode = p_next_inode(),
                    Children2 = gb_trees:insert(Filename, NewInode, Children),
                    mnesia:write(Parent#edfs_rec{children=Children2}),
                    mnesia:write(#edfs_rec{inode=NewInode, type=regular, children=0}),
                    {ok, NewInode};
                true ->
                    {error, eexist}
            end;
        [_] ->
            {error, enotdir}
    end.

p_mv(SrcPath,DestPath) ->
    SrcParts = filename:split(SrcPath),
    DestParts = filename:split(DestPath),
    {SrcParentParts,[SrcFilename]} = lists:split(length(SrcParts)-1, SrcParts),
    {DestParentParts,[DestFilename]} = lists:split(length(DestParts)-1, DestParts),

    mnesia:activity(transaction, fun()->

        case {p_get_inode(SrcParentParts), p_get_inode(SrcPath)} of
            {{ok, SrcParentInode},{ok, SrcInode}} ->
                case p_get_inode(DestParentParts) of
                    {ok,DestParentInode} ->
                        p_mv2(SrcParentInode, SrcInode, SrcFilename, DestParentInode, DestFilename);
                    Error -> Error
                end;
            _ -> {error, enoent}
        end
    end).
p_mv2(SrcParentInode, SrcInode, SrcFilename, DestParentInode, DestFilename) ->
    case mnesia:wread({edfs_rec, DestParentInode}) of
        [Rec=#edfs_rec{type=directory, children=Children}] ->
            case gb_trees:lookup(DestFilename, Children) of
                none ->
                    ok=p_unlink(SrcParentInode,SrcFilename),
                    Children2 = gb_trees:insert(DestFilename, SrcInode, Children),
                    mnesia:write(Rec#edfs_rec{children=Children2});
                {_,Inode} ->
                    case p_stat(Inode) of
                        {ok, {_, directory}} ->
                            p_mv2(SrcParentInode, SrcInode, SrcFilename, Inode, SrcFilename);
                        _ ->
                            {error, eexist}
                    end
            end;
        _ -> {error, enotdir}
    end.


p_register_blob(BlobID, Host) ->
    mnesia:activity(transaction, fun()->
        case mnesia:wread({edfs_blob, BlobID}) of
            [Blob=#edfs_blob{hosts=Hosts}] ->
                mnesia:write(Blob#edfs_blob{hosts=[Host|Hosts]});
            [] ->
               {error, enoent}
        end
    end).


p_rm(Path) ->
    PathParts = filename:split(Path),
    {ParentParts,[Filename]} = lists:split(length(PathParts)-1, PathParts),
    case p_get_inode(ParentParts) of
        {ok, ParentInode} -> p_rm(ParentInode,Filename);
        Error -> Error
    end.
p_rm(ParentInode, Filename) ->
    case mnesia:dirty_read(edfs_rec, ParentInode) of
        [#edfs_rec{type=directory, children=Children}] ->
            case gb_trees:lookup(Filename, Children) of
                {value, Inode} ->
                    case p_rmf(Inode) of
                        ok -> p_unlink(ParentInode, Filename);
                        Error -> Error
                    end;
                none -> ok
            end;
        _ -> {error, enoent}
    end.
p_unlink(ParentInode, Filename) ->
    mnesia:activity(transaction, fun()->
        case mnesia:read(edfs_rec, ParentInode) of
            [Parent=#edfs_rec{type=directory, children=Children}] ->
                Children2 = gb_trees:delete(Filename, Children),
                mnesia:write(Parent#edfs_rec{children=Children2});
            _ -> ok
        end
    end).
p_rmf(Inode) ->
    case mnesia:dirty_read(edfs_rec, Inode) of
        [#edfs_rec{type=directory, children=Children}] ->
            p_each_children(fun({_,ChildInode}) ->
                p_rmf(ChildInode)
            end, Children),
            mnesia:activity(transaction, fun()->
                mnesia:delete({edfs_rec, Inode}),
                mnesia:write(#edfs_inodes{inode=Inode})
            end);
        [#edfs_rec{type=regular}] ->
            mnesia:activity(transaction, fun()->
                case p_blobs(Inode) of
                    {ok, Blobs} ->
                        [edfs_slave:delete(Blob) || Blob <- Blobs],
                        [mnesia:delete({edfs_blob,BlobID}) || {BlobID, _Hosts} <- Blobs],
                        ok;
                    _ -> ok
                end,
                mnesia:delete({edfs_rec, Inode}),
                mnesia:write(#edfs_inodes{inode=Inode})
            end);
        [] -> {error, enoent}
    end.


p_stat(Path) when is_binary(Path)->
    mnesia:activity(ets, fun()->
        case p_get_inode(Path) of
            {ok, Inode} -> p_stat(Inode);
            Error -> Error
        end
    end);
p_stat(Inode) when is_integer(Inode) ->
    mnesia:activity(ets, fun()->
        case mnesia:read(edfs_rec, Inode) of
            [#edfs_rec{type=Type}] -> {ok, {Inode, Type}};
            [] -> {error, enoent}
        end
    end).


%% ===================================================================
%% UTILS
%% ===================================================================

p_master_node() ->
    case application:get_env(edfs,master) of
        {ok, Node} -> Node;
        undefined -> exit({error,{env_not_set,{edfs,master}}})
    end.


p_next_inode() ->
    mnesia:activity(transaction, fun()->
        case mnesia:first(edfs_inodes) of
            '$end_of_table' ->
                [#edfs_prop{value=Inode}] = mnesia:wread({edfs_prop, inode}),
                NextInode=Inode+1,
                mnesia:write(#edfs_prop{key=inode, value=NextInode}),
                NextInode;
            NextInode ->
                mnesia:delete({edfs_inodes, NextInode}),
                NextInode
        end
    end).


p_each_children(F,Children) ->
    Iter = gb_trees:iterator(Children),
    p_each_iter(F,gb_trees:next(Iter)).

p_each_iter(F,{K,V,I}) ->
    F({K,V}),
    p_each_iter(F, gb_trees:next(I));
p_each_iter(_F,none) ->
    ok.


p_make_blob_ids(Inode, BlobsCount) ->
    [ {Inode,Part} || Part <- lists:seq(1, BlobsCount)].
