-module(erlduce_job).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

% public api
-export([
    start_link/2,
    stop/2,
    update_counter/2,
    update_counter/3,
    wait/1
]).

% internal mapred
-export([
    slave_req_input/2,
    slave_ack_input/2,
    slave_ack_input_all/2
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
    blobs_queue :: ets:tid(),
    blobs_taken :: ets:tid(),

    counters :: ets:tid(),
    dir :: string(),
    wait = [] :: list(pid()),

    start_time :: erlang:timestamp(),
    input_len = 0 :: integer(),
    total_done = 0 :: integer(),
    last_progress = 0 :: integer(),
    progress :: function() | undefined
}).

-define(DEFAULT_COUNTERS, [
    map_time,
    shuffle_time,
    reduce_time,
    input_bytes
    % network_send_bytes
]).


start_link(Nodes, JobSpec) ->
    gen_server:start_link(?MODULE, {Nodes, JobSpec}, []).


stop(Pid, Reason) ->
    gen_server:cast(Pid, {stop, Reason}).

update_counter(Pid, IncList) ->
    gen_server:cast(Pid, {update_counter, IncList}).

update_counter(Pid, Key, Incr) ->
    gen_server:cast(Pid, {update_counter, [{Key, Incr}]}).

wait(Pid) ->
    gen_server:call(Pid, wait, infinity).


slave_req_input(Pid, From) ->
    gen_server:cast(Pid, {slave_req_input, From, erlduce_utils:host()}).


slave_ack_input(Pid, BlobID) ->
    gen_server:cast(Pid, {slave_ack_input, BlobID}).

slave_ack_input_all(Pid, BlobID) ->
    gen_server:cast(Pid, {slave_ack_input_all, BlobID}).


%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init({Nodes, JobSpec0}) ->
    StartTime = os:timestamp(),
    process_flag(trap_exit, true),

    SlaveList = lists:foldl(fun({Node,Slots}, AccNodes)->
        lists:foldl(fun(_,Acc)-> [Node|Acc] end, AccNodes, lists:seq(1, Slots))
    end, [], Nodes),
    SlaveNodes = lists:usort(SlaveList),

    {RunID, JobID} = proplists:get_value(id, JobSpec0),
    {ok, BaseDir} = application:get_env(erlduce_slave, dir),
    DirParts = [erlduce_utils:any_to_list(Part) || Part <- [BaseDir, RunID, JobID]],
    Dir = filename:join(DirParts),

    SlaveLen = length(SlaveList),
    JobSpec1 = [{master, self()}, {dir, Dir} | JobSpec0],
    JobSpec = case lists:keymember(partition, 1, JobSpec1) of
        true -> JobSpec1;
        false -> [{partition, fun(X)-> erlang:phash2(X,SlaveLen) end} | JobSpec1]
    end,

    Slaves = erlduce_utils:pmap(fun(Node)->
        {ok, Pid} = erlduce_slave:start_link(Node,JobSpec),
        Pid
    end, SlaveList),

    erlduce_utils:pmap(fun(Pid) -> ok=erlduce_slave:run(Pid,Slaves) end, Slaves),

    BlobsQueue = ets:new(blobs_queue,[set,private]),
    BlobsTaken = ets:new(blobs_taken,[set, private]),
    Blobs = p_prepare_input(proplists:get_value(input, JobSpec)),
    ets:insert(BlobsQueue,Blobs),

    Counters = ets:new(counters,[set,private]),
    InitCounters = [ {Key,0} || Key <- ?DEFAULT_COUNTERS],
    ets:insert(Counters, InitCounters),

    {ok, #state{
        slaves = Slaves,
        slave_nodes = SlaveNodes,
        slaves_count = SlaveLen,
        blobs_queue = BlobsQueue,
        blobs_taken = BlobsTaken,
        counters = Counters,
        dir = Dir,
        start_time = StartTime,
        input_len = length(Blobs),
        progress = proplists:get_value(progress, JobSpec)
    }}.


handle_call( wait, From, State=#state{wait=Wait}) ->
    {noreply, State#state{ wait=[From|Wait] }};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {slave_ack_input,BlobID}, State=#state{ blobs_taken=BlobsTaken, slaves=Slaves, slaves_count=Count, total_done=TotalDone }) ->
    State2 = case ets:update_counter(BlobsTaken, BlobID, {2,1}) of
        Count ->
            ets:delete(BlobsTaken, BlobID),
            p_check_is_done(self(), BlobsTaken, Slaves),
            LastProg = p_progress(State),
            State#state{ total_done=TotalDone+1, last_progress=LastProg };
        _ -> State
    end,
    {noreply, State2};
handle_cast( {slave_ack_input_all, BlobID}, State=#state{ blobs_taken=BlobsTaken, slaves=Slaves, total_done=TotalDone }) ->
    ets:delete(BlobsTaken, BlobID),
    p_check_is_done(self(), BlobsTaken, Slaves),
    LastProg = p_progress(State),
    {noreply, State#state{ total_done=TotalDone+1, last_progress=LastProg }};


handle_cast( {slave_req_input, Pid, Host}, State=#state{ blobs_queue=BlobsQueue, blobs_taken=BlobsTaken }) ->
    erlduce_slave:input(Pid,p_get_input(Host,BlobsQueue,BlobsTaken)),
    {noreply, State};

handle_cast( {update_counter, IncList}, State=#state{ counters=Counters }) ->
    [ ets:update_counter(Counters, Key, Incr) || {Key, Incr} <- IncList],
    {noreply, State};

handle_cast( {stop, Reason}, State) ->
    {stop, Reason, State};

handle_cast( _Msg, State) ->
    {noreply, State}.



handle_info( {'EXIT', From, Reason}, State) ->
    case Reason of
        normal -> {noreply, State};
        _      -> {stop, {error, {From,Reason}}, State}
    end;
handle_info( _Info, State) ->
    {noreply, State}.



terminate( normal, #state{  slave_nodes=Nodes, dir=Dir, wait=Wait, counters=Counters, start_time=StartTime }) ->
    Stats0 = ets:tab2list(Counters),
    TotalTime = timer:now_diff(os:timestamp(), StartTime)/1000000,
    Stats1 =  lists:keysort(1, Stats0),
    Stats = [{job_time, TotalTime} | Stats1],
    erlduce_utils:pmap(fun(Node)-> rpc:call(Node, erlduce_utils, rmdir, [Dir]) end, Nodes),
    erlduce_utils:pmap(fun(From)-> gen_server:reply(From, {ok, Stats}) end, Wait),
    ok;
terminate( _Reason, _State) ->
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


p_check_is_done(Master, BlobsTaken, Slaves) ->
    case ets:info(BlobsTaken,size) of
        0 ->
            spawn_link(fun()->
                erlduce_utils:pmap(fun(Pid) -> erlduce_slave:done(Pid) end, Slaves),
                erlduce_job:stop(Master,normal)
            end);
        _ -> ok
    end.


p_progress(#state{input_len=InpLen, total_done=DoneLen, last_progress=LastProg, progress=ProgFun}) ->
    Prog =  trunc((DoneLen/InpLen) *10),
    if
        Prog > LastProg andalso is_function(ProgFun) ->
            spawn(fun()->ProgFun(Prog*10) end);
        true -> ok
    end,
    Prog.
