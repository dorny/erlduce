-module(erlduce_slave).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/2,
    run/2,
    input/2,
    merge/3,
    done/1
]).

% private exports
-export([
    p_map_worker/8
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
    master::pid(),
    slaves = undefined :: list(pid()),

    job_id :: tuple(),
    task_index = undefined :: integer(),

    map :: function(),
    reduce :: function(),
    partition :: function(),
    output :: function(),
    write :: function(),

    sem :: pid(),
    mem_threshold = undefined :: number(),

    files = [] :: list(),
    dir = undefined ::string()
}).


start_link(Node,  JobSpec) ->
    rpc:call(Node, gen_server, start, [?MODULE, JobSpec, []]).


run(Pid, Slaves) ->
    gen_server:call(Pid, {run, Slaves}).


input(Pid, Blob) ->
    gen_server:cast(Pid, {input, Blob}).


merge(Pid, BlobID, Items) ->
    gen_server:cast(Pid, {merge, BlobID, Items}).


done(Pid) ->
    gen_server:call(Pid,done).

%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init(JobSpec) ->
    Master = proplists:get_value(master, JobSpec),
    link(Master),
    erase(),
    {ok, #state{
        master = Master,
        job_id = proplists:get_value(id, JobSpec),
        map = proplists:get_value(map, JobSpec),
        reduce = proplists:get_value(reduce, JobSpec),
        partition = proplists:get_value(partition, JobSpec),
        output = proplists:get_value(output, JobSpec),
        write = p_write_fun(proplists:get_value(combine, JobSpec)),
        sem = erlduce_utils:sem_new(1)
    }}.

handle_call( {run,Slaves}, _From, State=#state{master=Master, job_id=JobID }) ->
    erlduce_job:slave_req_input(Master, self()),
    % TaskIndex
    {RunID, JobIndex} = JobID,
    TaskIndex = p_list_index(self(),Slaves),
    % Dir
    {ok, BaseDir} = application:get_env(erlduce_slave, dir),
    JobPart = [erlduce_utils:any_to_list(Part) || Part <- [RunID,"-",JobIndex,"-",TaskIndex]],
    Dir = filename:join([BaseDir, JobPart]),
    erlduce_utils:mkdirp(Dir),
    % MemThreshold
    {ok, MemThresholdStr} = application:get_env(erlduce_slave, emulator_memory_flush_threshold),
    MemThreshold0 = erlduce_utils:parse_size(MemThresholdStr),
    Node = node(),
    LocalCount = lists:foldl(fun(Pid,Sum)->
        case node(Pid) of
            Node -> Sum+1;
            _ -> Sum
        end
    end, 0, Slaves),
    MemThreshold = MemThreshold0 * LocalCount,
    {reply, ok, State#state{ task_index=TaskIndex, slaves=Slaves, dir=Dir, mem_threshold=MemThreshold }};

handle_call( done, _From, State=#state{ files=[], output=Output, task_index=Idx }) ->
    Items = p_get_items(),
    Arg = Output(open,Idx),
    Output(close, Output(Items,Arg)),
    {stop, normal, ok, State};

handle_call( done, _From, State=#state{ files=Files, output=Output, task_index=Idx }) ->
    {stop, normal, ok, State};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {input, eof}, State=#state{ dir=Dir, files=Files, sem=Sem}) ->
    State2 = case Files of
        [] -> State;
        _ ->
            erlduce_utils:sem_wait(Sem),
            File = p_flush_to_disk(Dir, length(Files)),
            erlduce_utils:sem_signal(Sem),
            State#state{ files=[File|Files] }
    end,
    {noreply, State};
handle_cast( {input, Blob}, State=#state{ master=Master, slaves=Slaves, sem=Sem, write=Write, map=Map, partition=Part }) ->
    spawn_link(?MODULE, p_map_worker, [self(),Master,Blob,Slaves,Sem,Write,Map,Part]),
    {noreply, State};

handle_cast( {merge, BlobID, Items}, State=#state{
        master=Master, write=Write, sem=Sem, mem_threshold=MemThreshold, dir=Dir, files=Files }) ->
    erlduce_utils:sem_wait(Sem),
    [Write(Item) || Item <- Items],
    MemUsed = erlang:memory(total),
    State2 = if
        MemUsed > MemThreshold ->
            File = p_flush_to_disk(Dir, length(Files)),
            State#state{ files=[File|Files] };
        true -> State
    end,
    erlduce_utils:sem_signal(Sem),
    erlduce_job:slave_ack_input(Master,BlobID),
    {noreply,State2};

handle_cast( _Msg, State) ->
    {noreply, State}.


handle_info( _Info, State) ->
    {noreply, State}.


terminate( _Reason, #state{ dir=Dir }) ->
    erlduce_utils:rmdir(Dir),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% PRIVATE
%% ===================================================================

p_write_fun(undefined) ->
    fun({K,V})->
        case get(K) of
            undefined -> put(K,[V]);
            L -> put(K, [V|L])
        end
    end;
p_write_fun(Combine) when is_function(Combine) ->
    fun({K,V})->
        case get(K) of
            undefined -> put(K,V);
            Old -> put(K, Combine(K, V, Old))
        end
    end.


p_get_items() ->
    lists:keysort(1, erlang:erase()).


p_map_worker(Slave, Master, {BlobID, Path, Hosts}, Slaves, Sem, Write, Map, Part) ->
    case edfs:read({BlobID,Hosts}) of
        {ok, Bytes} ->
            erlduce_utils:sem_wait(Sem),
            erlduce_job:slave_req_input(Master, Slave),
            erlang:erase(),
            Map(Path,Bytes,Write),
            Items = erlang:erase(),
            erlduce_utils:sem_signal(Sem),
            p_combine_worker_dispatch(BlobID, Items, Slaves, Part);
        Error ->
            lager:error("Failed to read blob: ~p",[Error])
    end.
p_combine_worker_dispatch(BlobID, Items, Slaves, Partition) ->
    Len = length(Slaves),
    Acc0 = array:new(Len, [{default, []}]),
    Buf0 = lists:foldl(fun(Rec={K,V},Acc)->
        I = Partition(K) rem Len,
        L = array:get(I, Acc),
        array:set(I, [Rec|L], Acc)
    end, Acc0, Items),
    Buf = lists:zip(Slaves,array:to_list(Buf0)),
    erlduce_utils:pmap(fun({Pid,Items})->
        erlduce_slave:merge(Pid, BlobID, Items)
    end, Buf),
    ok.


p_flush_to_disk(Dir, Idx) ->
    Buf = p_get_items(),
    File = filename:join(Dir, integer_to_list(Idx)),
    ok=prim_file:write_file(File, term_to_binary(Buf)),
    File.


p_list_index(E, L) -> p_list_index(E,L,0).
p_list_index(E, [E|T], Pos) -> Pos;
p_list_index(E, [_|T], Pos) -> p_list_index(E,T,Pos+1);
p_list_index(E, [], Pos) -> false.
