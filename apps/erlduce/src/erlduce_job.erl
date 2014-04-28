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
    blobs_queue :: ets:tid(),
    blobs_taken :: ets:tid()
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
    process_flag(trap_exit, true),
    {RunID, JobIndex} = proplists:get_value(id, JobSpec0),
    {ok, BaseDir} = application:get_env(erlduce_slave, dir),

    SlaveList = lists:foldl(fun({Node,Slots}, AccNodes)->
        lists:foldl(fun(_,Acc)-> [Node|Acc] end, AccNodes, lists:seq(1, Slots))
    end, [], Nodes),

    SlaveLen = length(SlaveList),
    JobSpec1 = [{master, self()} | JobSpec0],
    JobSpec = case lists:member(partition, JobSpec1) of
        true -> JobSpec1;
        false -> [{partition, fun(X)-> erlang:phash2(X,SlaveLen) end} | JobSpec1]
    end,

    Slaves = erlduce_utils:pmap(fun(Node)->
        {ok, Pid} = erlduce_slave:start_link(Node,JobSpec),
        Pid
    end, SlaveList),

    erlduce_utils:pmap(fun(Pid) -> ok=erlduce_slave:run(Pid,Slaves) end, Slaves),

    BlobsQueue = ets:new(blobs_queue,[set]),
    BlobsTaken = ets:new(blobs_taken,[set]),
    Blobs = p_prepare_input(proplists:get_value(input, JobSpec)),
    ets:insert(BlobsQueue,Blobs),

    {ok, #state{
        slaves = Slaves,
        slave_nodes = SlaveList,
        slaves_count = SlaveLen,
        blobs_queue = BlobsQueue,
        blobs_taken = BlobsTaken
    }}.


handle_call( wait, From, State=#state{wait=Wait}) ->
    {noreply, State#state{ wait=[From|Wait] }};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.



handle_cast( {slave_ack_input,BlobID},
        State=#state{ blobs_taken=BlobsTaken, slaves=Slaves, slaves_count=Count, wait=Wait }) ->
    End = case ets:update_counter(BlobsTaken, BlobID, {2,1}) of
        Count ->
            ets:delete(BlobsTaken, BlobID),
            case ets:info(BlobsTaken,size) of
                0 -> true;
                _ -> false
            end;
        _ -> false
    end,
    case End of
        true ->
            erlduce_utils:pmap(fun(Pid) -> erlduce_slave:done(Pid) end, Slaves),
            {stop, normal, State};
        false -> {noreply, State}
    end;

handle_cast( {slave_req_input, Pid, Host}, State=#state{ blobs_queue=BlobsQueue, blobs_taken=BlobsTaken }) ->
    erlduce_slave:input(Pid,p_get_input(Host,BlobsQueue,BlobsTaken)),
    {noreply, State};

handle_cast( _Msg, State) ->
    {noreply, State}.



handle_info( {'EXIT', From, Reason}, State) ->
    case Reason of
        normal -> {noreply, State};
        _      -> {stop, {error, {From,Reason}}, State}
    end;
handle_info( _Info, State) ->
    {noreply, State}.



terminate( normal, State=#state{wait=Wait, slave_nodes=Nodes}) ->
    erlduce_utils:pmap(fun(From)-> gen_server:reply(From, ok) end, Wait),
    ok;
terminate( _Reason, State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% PRIVATE
%% ===================================================================

p_prepare_input(InputFun) when is_function(InputFun) ->
    {ok,Blobs} = InputFun(),
    lists:foreach(fun(Blob={BlobID, _Path, Hosts})->
        lists:foreach(fun(Host)->
            Key = {host, Host},
            case get(Key) of
                undefined -> put(Key,[Blob]);
                Acc -> put(Key, [BlobID | Acc])
            end
        end, Hosts)
    end, Blobs),
    Blobs;
p_prepare_input(_) -> exit({error, {input, badarg}}).


p_get_input(Host,BlobsQueue,BlobsTaken) ->
    Key = {host,Host},
    Blob = case get(Key) of
        undefined -> p_get_input_first(BlobsQueue);
        List ->
            case p_get_input2(List, BlobsQueue) of
                {[], B} -> erase(Key), B;
                {T, B} -> put(Key,T), B
            end
    end,
    case Blob of
        eof -> eof;
        {BlobID,_,_} ->
            ets:delete(BlobsQueue, BlobID),
            ets:insert(BlobsTaken, {BlobID,0})
    end,
    Blob.
p_get_input2([BlobID | T], Tid) ->
    case ets:lookup(Tid, BlobID) of
        [Blob] -> {T,Blob};
        _ -> p_get_input2(T, Tid)
    end;
p_get_input2(T=[], Tid) ->
    {T, p_get_input_first(Tid)}.
p_get_input_first(Tid) ->
    case ets:first(Tid) of
        '$end_of_table' -> eof;
        Key ->
            [Blob] = ets:lookup(Tid,Key),
            Blob
    end.
