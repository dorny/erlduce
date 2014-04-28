-module(erlduce_job).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

% public api
-export([
    start_link/2,
    wait/1
]).

% internal mapred
-export([
    slave_req_input/2,
    slave_ack_input/2
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
    slaves :: list(pid()),
    slave_nodes :: list(atom()),
    slaves_count :: integer(),
    wait = [] :: list(pid()),
    blobs :: ets:tid(),
    dir :: string()
}).

-record(blob, {
    id :: term(),
    path :: binary(),
    hosts :: list(atom()),
    taken = false :: boolean(),
    ack = 0 :: integer()
}).


start_link(Nodes, JobSpec) ->
    gen_server:start_link(?MODULE, {Nodes, JobSpec}, []).


wait(Pid) ->
    gen_server:call(Pid, wait, infinity).


slave_req_input(Pid, From) ->
    gen_server:cast(Pid, {slave_req_input, From, erlduce_utils:host()}).


slave_ack_input(Pid, BlobID) ->
    gen_server:cast(Pid, {slave_ack_input, BlobID}).


%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init({Nodes, JobSpec0}) ->

    {RunID, JobIndex} = proplists:get_value(id, JobSpec0),
    {ok, BaseDir} = application:get_env(erlduce_slave, dir),
    Dir = filename:join([BaseDir, RunID, JobIndex]),

    SlaveNodes = lists:foldl(fun({Node,Slots}, AccNodes)->
        lists:foldl(fun(_,Acc)-> [Node|Acc] end, AccNodes, lists:seq(1, Slots))
    end, [], Nodes),

    JobSpec = [{master, self()} | JobSpec0],
    Slaves = erlduce_utils:pmap(fun(Node)->
        {ok, Pid} = erlduce_slave:start_link(Node,JobSpec),
        Pid
    end, SlaveNodes),
    erlduce_utils:pmap(fun(Pid) -> ok=erlduce_slave:run(Pid,Slaves) end, Slaves),

    BlobsTid = ets:new(blobs,[set,{keypos, #blob.id}]),
    Blobs = p_prepare_input(proplists:get_value(input, JobSpec)),
    ets:insert(BlobsTid,Blobs),

    {ok, #state{
        slaves = Slaves,
        slave_nodes = Nodes,
        slaves_count = length(Slaves),
        blobs = BlobsTid,
        dir = Dir
    }}.


handle_call( wait, From, State=#state{wait=Wait}) ->
    {noreply, State#state{ wait=[From|Wait] }};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {slave_ack_input,BlobID},
        State=#state{ blobs=Tid, slaves=Slaves, slave_nodes=Nodes, slaves_count=Count, wait=Wait, dir=Dir }) ->
    End = case ets:update_counter(Tid, BlobID, {#blob.ack, 1}) of
        Count ->
            ets:delete(Tid, BlobID),
            case ets:first() of
                '$end_of_table' -> true;
                _ -> false
            end;
        _ -> false
    end,
    case End of
        true ->
            erlduce_utils:pmap(fun(Pid) -> erlduce_slave:done(Pid) end, Slaves),
            erlduce_utils:pmap(fun(Node) -> rpc:call(Node, erlduce_utils, rmdir, [Dir]) end, Nodes),
            erlduce_utils:pmap(fun(From) -> gen_server:reply(From, ok) end, Wait),
            {stop, State};
        false -> {noreply, State}
    end;

handle_cast( {slave_req_input, Pid, Host}, State=#state{blobs=BlobsTid}) ->
    erlduce_slave:input(Pid,p_get_input(Host,BlobsTid)),
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

p_prepare_input(InputFun) when is_function(InputFun) ->
    {ok,Blobs} = InputFun(),
    lists:foldl(fun(Blob={BlobID, _Path, Hosts})->
        lists:foreach(fun(Host)->
            Key = {host, Host},
            case get(Key) of
                undefined -> put(Key,[Blob]);
                Acc -> put(Key, [BlobID | Acc])
            end
        end, Hosts)
    end, Blobs),
    [ #blob{
        id=BlobID,
        path=Path,
        hosts=Hosts
    } || {BlobID, Path, Hosts} <- Blobs];
p_prepare_input(_) -> exit({error, {input, badarg}}).


p_get_input(Host,BlobsTid) ->
    Blob = case get({host,Host}) of
        undefined -> p_get_input_first(BlobsTid);
        List ->
            case p_get_input2(List, BlobsTid) of
                {[], B} -> B;
                {T, B} -> put({host,Host},T), B
            end
    end,
    case Blob of
        eof -> eof;
        #blob{id=BlobID, path=Path, hosts=Hosts} ->
            true=ets:update_element(BlobsTid, BlobID, {#blob.taken, true}),
            {BlobID, Path, Hosts}
    end.
p_get_input2([BlobID | T], Tid) ->
    case ets:lookup(Tid, BlobID) of
        [Blob=#blob{taken=false}] -> {T,Blob};
        _ -> p_get_input2(T, Tid)
    end;
p_get_input2(T=[], Tid) ->
    {T, p_get_input_first(Tid)}.
p_get_input_first(Tid) ->
    case ets:first(Tid) of
        '$end_of_table' -> eof;
        Key ->
            [Blob] = ets:lookup(Key),
            Blob
    end.
