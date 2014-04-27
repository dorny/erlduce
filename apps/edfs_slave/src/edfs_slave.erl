-module(edfs_slave).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/0,
    read/1,
    write/2,
    delete/1,
    format/1,
    disk_check/1
]).

%% private exports
-export([
    p_write_task/5
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
    status :: up | down,
    read_sem :: pid(),
    write_sem :: pid(),
    work_dir :: iolist(),
    host :: atom(),
    mounted_on :: string()
}).

-define(NAME(Host), {?MODULE,erlduce_utils:node(?MODULE,Host)}).


start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).


read({BlobID,Hosts}) ->
    Localhost = erlduce_utils:host(),
    Hosts2 = case lists:member(Localhost, Hosts) of
        true -> [Localhost | lists:delete(Localhost, Hosts)];
        false -> Hosts
    end,
    read(BlobID,Hosts2,[]).

read(BlobID, [Host| Hosts], Errors) ->
    case p_read(BlobID, Host) of
        {ok, Bytes} -> {ok, Bytes};
        Err -> read(Hosts, BlobID, [Err | Errors])
    end;
read(BlobID,[],[]) ->
    {error, {no_hosts,BlobID}};
read(_BlobID,[],Errors) ->
    {error, Errors}.
p_read(BlobID, Host) when is_atom(Host) ->
    case gen_server:call(?NAME(Host), {read, BlobID}) of
        {ok, {file, Filename}} ->
            prim_file:read_file(Filename);
        {ok, {pid, Pid}} ->
            receive
                {Pid, Resp} -> Resp
            end;
        Error -> Error
    end.


write({BlobID,Hosts}, Bytes) ->
    Res = erlduce_utils:pmap(fun(Host)->
        p_write(BlobID, Bytes, Host)
    end, Hosts),
    case lists:member(ok, Res)  of
        true -> ok;
        false -> {error, failed_to_write_bytes}
    end.
p_write(BlobID, Bytes, Host) ->
    case gen_server:call(?NAME(Host), {write, BlobID}) of
        {ok, {file, Filename}} ->
            p_write_file_local(BlobID, Filename, Bytes, Host);
        {ok, {pid, Pid}} ->
            Pid ! {self(), Bytes},
            receive
                {Pid, Resp} -> Resp
            end;
        Error -> Error
    end.


delete({BlobID, Hosts}) ->
    Nodes = [erlduce_utils:node(?MODULE,Host) || Host <- Hosts ],
    gen_server:abcast(Nodes, ?MODULE, {delete, BlobID}).


format(Nodes) ->
    gen_server:multi_call(Nodes, ?MODULE, format).


disk_check(Nodes) when is_list(Nodes) ->
    gen_server:multi_call(Nodes, ?MODULE, disk_check);
disk_check(Node) when is_atom(Node) ->
    gen_server:call({?MODULE, Node}, disk_check).



%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init(_Args) ->
    application:load(edfs),
    {ok, DiskCheck} = application:get_env(os_mon, disk_space_check_interval),
    timer:apply_interval(DiskCheck*60*1000, ?MODULE, disk_check, [node()]),

    {ok, Reads} = application:get_env(edfs, max_read_threads),
    {ok, Writes} = application:get_env(edfs, max_write_threads),
    {ok, WorkDir} = application:get_env(edfs, dir),

    MountedOn = os:cmd("df '"++WorkDir++"' | sed -n '2p'  | awk '{print $6}'"),

    {ok, #state{
        status = up,
        read_sem = erlduce_utils:sem_new(Reads),
        write_sem = erlduce_utils:sem_new(Writes),
        work_dir = WorkDir,
        host = erlduce_utils:host(),
        mounted_on = MountedOn
    }}.


handle_call( {read, BlobID}, {Pid, _}, State=#state{ read_sem=Sem, host=Host, work_dir=WorkDir}) ->
    Filename = blob_filename(BlobID, WorkDir),
    case erlduce_utils:host(node(Pid)) of
        Host ->
            {reply , {ok, {file, Filename}}, State};
        _ ->
            Worker = spawn(fun()->
                link(Pid),
                erlduce_utils:sem_wait(Sem),
                Result = prim_file:read_file(Filename),
                erlduce_utils:sem_signal(Sem),
                Pid ! {self(), Result}
            end),
            {reply , {ok, {pid, Worker}}, State}
    end;

handle_call( {write, BlobID}, {Pid, _}, State=#state{ write_sem=Sem, host=Host, work_dir=WorkDir}) ->
    Filename = blob_filename(BlobID, WorkDir),
    case erlduce_utils:host(node(Pid)) of
        Host ->
            {reply , {ok, {file, Filename}}, State};
        _ ->
            Worker = spawn(?MODULE,p_write_task,[BlobID, Filename, Pid, Host, Sem]),
            {reply , {ok, {pid, Worker}}, State}
    end;

handle_call( format, _From, State=#state{ work_dir=WorkDir}) ->
    BlobsDir = filename:join(WorkDir, <<"blobs">>),
    Resp = case file:list_dir(BlobsDir) of
        {ok, Filenames} ->
            [prim_file:delete(filename:join(BlobsDir, Filename)) || Filename <- Filenames],
            ok;
        {error, enoent} ->
            file:make_dir(BlobsDir),
            ok;
        Error -> Error
    end,
    {reply, Resp, State};

handle_call( disk_check, _From, State=#state{ status=Status, mounted_on=MountedOn}) ->
    Key = {disk_almost_full,MountedOn},
    Alarms = alarm_handler:get_alarms(),
    Status2 = case lists:keymember(Key, 1, Alarms) of
        true ->
            case Status of
                up -> edfs_master:node_down(node(), enospc);
                down -> ok
            end,
            down;
        false ->
            case Status of
                up -> ok;
                down -> edfs_master:node_up(node())
            end,
            up
    end,
    {reply, Status2, State#state{ status=Status2}};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( {delete, BlobID}, State=#state{ work_dir=WorkDir}) ->
    Filename = blob_filename(BlobID, WorkDir),
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


%% ===================================================================
%% PRIVATE
%% ===================================================================

blob_filename({Inode, Ord}, WorkDir) ->
    filename:join(WorkDir, integer_to_list(Inode)++"-"++integer_to_list(Ord)).


p_write_task(BlobID, Filename, Pid, Host, Sem) ->
    link(Pid),
    erlduce_utils:sem_wait(Sem),
    receive
        {Pid, Bytes} ->
            Result = prim_file:write_file(Filename, Bytes),
            erlduce_utils:sem_signal(Sem),
            case Result of
                ok ->
                    case edfs_master:register_blob(BlobID, Host) of
                        ok ->
                            Pid ! {self(), ok};
                        Error ->
                            prim_file:delete(Filename),
                            Pid ! {self(), Error}
                    end;
                Error -> Pid ! {self(), Error}
            end
    end.

p_write_file_local(BlobID, Filename, Bytes, Host) ->
    case prim_file:write_file(Filename, Bytes) of
        ok ->
            case edfs_master:register_blob(BlobID, Host) of
                ok -> ok;
                Error ->
                    prim_file:delete(Filename),
                    Error
            end;
        Error -> Error
    end.
