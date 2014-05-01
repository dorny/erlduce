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

-define( FLUSH_MIN_INTERVAL, 2*1000000).

-record(state, {
    master::pid(),
    slaves = undefined :: list(pid()),
    task_index = undefined :: integer(),

    map :: function(),
    combine :: function() | undefined,
    reduce :: function() | undefined,
    partition :: function() | undefined,
    output :: function(),
    write :: function(),

    sem_map :: pid(),
    sem_merge :: pid(),
    mem_threshold = undefined :: number(),
    last_flush = {0,0,0} :: erlang:timestamp(),

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
    gen_server:call(Pid,done, infinity).

%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init(JobSpec) ->
    Master = proplists:get_value(master, JobSpec),
    link(Master),

    Combine = proplists:get_value(combine, JobSpec),
    Reduce = proplists:get_value(reduce, JobSpec),
    Write = p_write_fun(Combine,Reduce),

    erase(),
    {ok, #state{
        master = Master,
        map = proplists:get_value(map, JobSpec),
        combine = Combine,
        reduce = Reduce,
        write = Write,
        partition = proplists:get_value(partition, JobSpec),
        output = proplists:get_value(output, JobSpec),
        sem_map = erlduce_utils:sem_new(1),
        sem_merge = erlduce_utils:sem_new(1),
        dir = proplists:get_value(dir, JobSpec)
    }}.

handle_call( {run,Slaves}, _From, State=#state{master=Master, dir=Dir }) ->
    erlduce_job:slave_req_input(Master, self()),
    % TaskIndex
    TaskIndex = p_list_index(self(),Slaves),
    % Dir
    Dir2 = filename:join(Dir, integer_to_list(TaskIndex)),
    erlduce_utils:mkdirp(Dir2),
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
    {reply, ok, State#state{ task_index=TaskIndex, slaves=Slaves, dir=Dir2, mem_threshold=MemThreshold }};

% MAP-ONLY
handle_call( done, _From, State=#state{ write=undefined }) ->
    {stop, normal, ok, State};

% Combine or Reduce in Memory
handle_call( done, _From, State=#state{ master=Master, files=[], task_index=Idx, reduce=Reduce, output=Output }) ->
    {ReduceTime, _} = timer:tc(fun()->
        p_reduce_mem(Idx, Reduce, Output)
    end),
    erlduce_job:update_counter(Master, reduce_time, ReduceTime),
    {stop, normal, ok, State};

% Combine or Reduce on Files
handle_call( done, _From, State=#state{ master=Master, dir=Dir, files=Files0, combine=Combine, reduce=Reduce, output=Output, task_index=Idx }) ->
    {ReduceTime, _} = timer:tc(fun()->
        Files = [ p_flush_to_disk(Dir, length(Files0)) | Files0],
        Fun = p_merge_files_fun( Combine, Reduce, Output({open, Idx})),
        Fun2 = erlduce_utils:merge_files(Files, Fun),
        Fun2(close)
    end),
    erlduce_job:update_counter(Master, reduce_time, ReduceTime),
    {stop, normal, ok, State};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {input, eof}, State=#state{ master=Master, dir=Dir, files=Files, sem_map=SemMap}) ->
    State2 = case Files of
        [] -> State;
        _ ->
            erlduce_utils:sem_wait(SemMap),
            {TmpWriteTime,File} = timer:tc(fun()->
                p_flush_to_disk(Dir, length(Files))
            end),
            erlduce_utils:sem_signal(SemMap),
            erlduce_job:update_counter(Master, reduce_time, TmpWriteTime),
            State#state{ files=[File|Files] }
    end,
    {noreply, State2};
handle_cast( {input, Blob}, State=#state{ master=Master, slaves=Slaves, sem_map=SemMap, sem_merge=SemMerge, write=Write, map=Map, partition=Part }) ->
    spawn_link(?MODULE, p_map_worker, [self(),Master,Blob,Slaves,{SemMap,SemMerge},Write,Map,Part]),
    {noreply, State};

handle_cast( {merge, BlobID, []}, State=#state{ master=Master }) ->
    erlduce_job:slave_ack_input(Master,BlobID),
    {noreply,State};

handle_cast( {merge, BlobID, Items}, State=#state{
        master=Master, write=Write, sem_merge=SemMerge, mem_threshold=MemThreshold, last_flush=LastFlush, dir=Dir, files=Files }) ->
    erlduce_utils:sem_wait(SemMerge),
    {Time, _} = timer:tc(fun()->
        [Write(Item) || Item <- Items], ok
    end),
    erlduce_utils:sem_signal(SemMerge),
    erlduce_job:update_counter(Master, reduce_time, Time),

    HighMem = p_check_mem(MemThreshold),
    TDiff = timer:now_diff(os:timestamp(), LastFlush),

    State2 = if
        HighMem andalso TDiff > ?FLUSH_MIN_INTERVAL ->
            {TmpWriteTime,File} = timer:tc(fun()->
                p_flush_to_disk(Dir, length(Files))
            end),
            erlduce_job:update_counter(Master, reduce_time, TmpWriteTime),
            State#state{ files=[File|Files], last_flush=os:timestamp() };
        true -> State
    end,
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

p_list_index(E, L) -> p_list_index(E,L,0).
p_list_index(E, [E|_T], Pos) -> Pos;
p_list_index(E, [_|T], Pos) -> p_list_index(E,T,Pos+1);
p_list_index(_E, [], _Pos) -> false.

p_write_fun(undefined,undefined) ->
    undefined;
p_write_fun(undefined,Reduce) when is_function(Reduce) ->
    p_write_acc_fun();
p_write_fun(Combine,undefined) when is_function(Combine) ->
    p_write_combine_fun(Combine).

p_write_acc_fun() ->
    fun({K,V})->
        case get(K) of
            undefined -> put(K,[V]);
            L -> put(K, [V|L])
        end,
        p_write_acc_fun()
    end.
p_write_combine_fun(Combine) ->
    fun({K,V})->
        case get(K) of
            undefined -> put(K,V);
            Old -> put(K, Combine(K, V, Old))
        end,
        p_write_combine_fun(Combine)
    end.


p_map_worker(Slave, Master, {BlobID, Path, Hosts}, Slaves, {SemMap,SemMerge}, Write, Map, Part) ->
    case edfs:read({BlobID,Hosts}) of
        {ok, Bytes} ->
            erlduce_utils:sem_wait(SemMap),
            erlduce_utils:sem_wait(SemMerge),
            erlduce_job:slave_req_input(Master, Slave),
            {MapTime, Items} = timer:tc(fun()->
                erlang:erase(),
                Map(Path,Bytes,Write),
                erlang:erase()
            end),
            erlduce_utils:sem_signal(SemMerge),
            erlduce_utils:sem_signal(SemMap),
            erlduce_job:update_counter(Master,[{input_bytes, byte_size(Bytes)}, {map_time, MapTime} ]),
            case Write of
                undefined -> erlduce_job:slave_ack_input_all(Master, BlobID);
                _ ->
                    {ShuffleTime,_} = timer:tc(fun()->
                        p_combine_worker_dispatch(BlobID, Items, Slaves, Part)
                    end),
                    erlduce_job:update_counter(Master, shuffle_time, ShuffleTime)
            end;
        Error ->
            exit({error,{BlobID, Error}})
    end.
p_combine_worker_dispatch(BlobID, Items, Slaves, Partition) ->
    Len = length(Slaves),
    Acc0 = array:new(Len, [{default, []}]),
    Buf0 = lists:foldl(fun(Rec={K,_V},Acc)->
        I = Partition(K) rem Len,
        L = array:get(I, Acc),
        array:set(I, [Rec|L], Acc)
    end, Acc0, Items),
    Buf = lists:zip(Slaves,array:to_list(Buf0)),
    erlduce_utils:pmap(fun({Pid,Data})->
        erlduce_slave:merge(Pid, BlobID, Data)
    end, Buf),
    ok.


p_check_mem(MemThreshold) ->
    MemUsed = erlang:memory(total),
    if
        MemUsed > MemThreshold -> true;
        true -> false
    end.


p_flush_to_disk(Dir, Idx) ->
    Res = p_flush_to_disk_2(Dir, Idx),
    erlang:garbage_collect(),
    Res.
p_flush_to_disk_2(Dir, Idx) ->
    Buf = lists:keysort(1, erase()),
    File = filename:join(Dir, integer_to_list(Idx)),
    Bin=[erlduce_utils:to_file_record(Item) || Item <- Buf],
    ok=prim_file:write_file(File, Bin),
    File.


p_reduce_mem(Idx,Reduce,Output) ->
    Items = case Reduce of
        undefined ->
            erlang:erase();
        Fun when is_function(Fun) ->
            lists:flatmap(fun({Key,Values}) -> Fun(Key,Values) end, erlang:erase())
    end,
    case Items of
        [] -> ok;
        _ ->
            Sorted = lists:keysort(1,Items),
            Output2 = lists:foldl( fun(Item,Fun) ->
                Fun(Item)
            end, Output({open,Idx}), Sorted),
            Output2(close),
            ok
    end.


p_merge_files_fun(Combine,undefined,Output) when is_function(Combine) ->
    p_combine_files_fun(Combine, Output);
p_merge_files_fun(undefined,Reduce,Output) when is_function(Reduce) ->
    p_reduce_files_fun(Reduce,Output).

p_combine_files_fun(Combine,Output) ->
    fun ({Key,[H|T]})->
            Value=lists:foldl(fun(Val,Acc)-> Combine(Key,Val,Acc) end, H, T),
            Output2 = Output({Key,Value}),
            p_combine_files_fun(Combine,Output2);
        (close) ->
            Output(close)
    end.

p_reduce_files_fun(Reduce, Output) ->
    fun ({Key,Values})->
            Output2 = Reduce(Key,Values,Output),
            p_reduce_files_fun(Reduce,Output2);
        (close) ->
            Output(close)
    end.
