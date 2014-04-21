-module(edfs_slave).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/0,
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

-record(state, {
    master :: atom,
    host :: atom,
    blobs :: string()
}).

-define( Node(Host), erlduce_utils:node(edfs,Host)).



start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).


write(Host, BlobID, Bytes) when is_atom(Host) ->
    gen_server:call({?MODULE, ?Node(Host)}, {write, BlobID, Bytes});

write(Hosts, BlobID, Bytes) when is_list(Hosts) ->
    Nodes = [?Node(Host) || Host <- Hosts],
    Resps = gen_server:multi_call(Nodes, ?MODULE, {write, BlobID, Bytes}),
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
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    {ok, #state{
        master = application:get_env(erlduce, master),
        host = erlduce_utils:host(),
        blobs = filename:join(WorkDir, "blobs")
    }}.

handle_call( {write, BlobID, Bytes}, From, State=#state{master=Master, host=Host, blobs=BlobsDir}) ->
    Filename = filename:join(BlobsDir, BlobID),
    case file:write_file(Filename, Bytes,[raw]) of
        ok  ->
            edfs:register_blob(BlobID,Host),
            {reply, ok, State};
        Err ->
            lager:error("Failed to write file ~p: ~p...",[Filename, Err]),
            {reply, Err, State}
    end;

handle_call( Request, From, State) ->
    {reply, ignored, State}.


handle_cast( {delete, BlobID}, State=#state{blobs=BlobsDir}) ->
    Filename = filename:join(BlobsDir, BlobID),
    file:delete(Filename),
    {noreply, State};

handle_cast( Msg, State) ->
    {noreply, State}.


handle_info( Info, State) ->
    {noreply, State}.


terminate(Reason,_State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
