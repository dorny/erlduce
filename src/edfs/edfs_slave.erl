-module(edfs_slave).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/0,
    read/2,
    write/3,
    delete/2
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


-define( Node(Host), erlduce_utils:node(edfs,Host)).



start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).


read(Host, BlobID) when is_atom(Host) ->
    case erlduce_utils:host() of
        Host -> p_read(BlobID);
        _ -> gen_server:call({?MODULE, ?Node(Host)}, {read, BlobID})
    end;
read([], BlobID) ->
    {error, {no_hosts,BlobID}};
read([Host| Hosts], BlobID) ->
    case read(Host, BlobID) of
        RespOk={ok,_} -> RespOk;
        _Err -> read(Hosts, BlobID)
    end.


write(Host, BlobID, Bytes) when is_atom(Host) ->
    case erlduce_utils:host() of
        Host -> p_write(BlobID, Bytes, Host);
        _ -> gen_server:call({?MODULE, ?Node(Host)}, {write, BlobID, Bytes})
    end;
write(Hosts, BlobID, Bytes) when is_list(Hosts) ->
    Resps = erlduce_utils:pmap(fun(Host)-> write(Host,BlobID,Bytes) end, Hosts),
    case lists:member(ok, Resps) of
        true -> ok;
        false -> {error, Resps}
    end.


delete(Host, BlobID) ->
    gen_server:cast({?MODULE, ?Node(Host)}, {delete, BlobID}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  GEN_SERVER
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Args) ->
    {ok, undefined}.


handle_call( {read, BlobID}, _From, State) ->
    {reply , p_read(BlobID), State};

handle_call( {write, BlobID, Bytes, Host}, _From, State) ->
    {reply , p_write(BlobID, Bytes, Host), State};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {delete, BlobID}, State) ->
    Filename = edfs_lib:blob_filename(BlobID),
    % use erlang internal prim_file module
    % it's the only way to delete a file on local filesystem, rather then on master's
    prim_file:delete(Filename),
    {noreply, State};

handle_cast( _Msg, State) ->
    {noreply, State}.


handle_info( _Info, State) ->
    {noreply, State}.


terminate( _Reason,_State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  PRIVATE
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_read(BlobID) ->
    Filename = edfs_lib:blob_filename(BlobID),
    prim_file:read_file(Filename).


p_write(BlobID, Bytes, Host) ->
    Filename = edfs_lib:blob_filename(BlobID),
    case file:write_file(Filename, Bytes,[raw,binary]) of
        ok  ->
            edfs:register_blob(BlobID,Host),
            ok;
        Err ->
            lager:error("Failed to write file ~p: ~p...",[Filename, Err]),
            {error, Err}
    end.
