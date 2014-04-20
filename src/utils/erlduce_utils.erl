-module(erlduce_utils).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    % application
    start_application/1,

    % host/node name utils
    host/0,
    host/1,
    node/2,

    master/0,
    run_at_master/3,

    % parallel map on single node
    pmap/2,

    % starting slave nodes
    start_slaves/2,
    start_slave/3,
    set_envs/2,

    % loading code for jobs
    tar_load_modules/1,
    code_load_modules/1,

    % compression
    encode/2,
    decode/2,

    % CLI
    resp/1,
    getopts/7,
    die/1,
    die/2
]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Application
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_application(App) ->
    start_ok(App, application:start(App, permanent)).

start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, {not_started, Dep}}) ->
    ok = start_application(Dep),
    start_application(App);
start_ok(App, {error, Reason}) ->
    erlang:error({app_start_failed, App, Reason}).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Hostname/Nodename utils
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

host() ->
    host(node()).
host(Node) when is_atom(Node) ->
    list_to_atom(after_char($@, atom_to_list(Node))).

node(Name, Host) when is_atom(Name), is_atom(Host) ->
    list_to_atom(atom_to_list(Name)++"@"++atom_to_list(Host)).

after_char(_, []) -> [];
after_char(Char, [Char|Rest]) -> Rest;
after_char(Char, [_|Rest]) -> after_char(Char, Rest).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Master
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

master() ->
    {ok, Master} = application:get_env(erlduce, master),
    Master.

run_at_master(M,F,A) ->
    Master = master(),
    Node = node(),
    case Master of
        Node -> erlang:apply(M, F, A);
        _ -> rpc:call(Master, M, F, A)
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Parallel map
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

pmap(F, L) ->
    Parent = self(),
    [receive {Pid, Result} -> Result end || Pid <- [spawn(fun() -> Parent ! {self(), F(X)} end) || X <- L]].



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Slave nodes
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_slaves(Name, Hosts) ->
    Master = self(),
    TrySlaves = erlduce_utils:pmap(fun(Host)->
        {Host, start_slave(Name,Host,Master)}
    end, Hosts),

    Slaves = lists:filtermap(fun
        ({Host, {error,_}}) -> false;
        ({Host, {ok, Node}}) -> {true, {Host, Node}}
    end, TrySlaves),
    Slaves.

start_slave(Name, Host, LinkTo) ->
    {ok, Master} = application:get_env(erlduce, master),
    Cookie = erlang:get_cookie(),
    Paths = lists:foldr(fun(P, Acc)-> lists:concat([Acc," \"",filename:absname(P),"\""]) end, "", code:get_path()),

    Res = slave:start(Host, Name, lists:concat([
        " -setcookie ", atom_to_list(erlang:get_cookie()),
        " -pa ", Paths
    ]), LinkTo, erl),

    case Res of
        {ok, Node} ->
            ErlduceEnvs = [{master, Master} | application:get_all_env(erlduce)],
            LagerEnvs = application:get_all_env(lager),
            ok = rpc:call(Node, erlduce_utils, set_envs, [erlduce,ErlduceEnvs]),
            ok = rpc:call(Node, erlduce_utils, set_envs, [lager,LagerEnvs]),
            ok = rpc:call(Node, lager, start, []),
            lager:info("started node: ~p", [Node]),
            Res;
        {error, Reason} ->
            lager:error("failed to start node ~p@~p: ~p", [Name, Host, Reason]),
            {error, Reason}
    end.

set_envs(App, Envs) ->
     lists:foreach(fun({K,V})->application:set_env(App, K, V) end, Envs),
     ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Code loading
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tar_load_modules(Src) ->
case erl_tar:extract(Src, [memory,compressed]) of
    {ok, FileList} ->
        Modules = lists:map(fun({Filename, Bin})->
            Mod = list_to_atom(filename:rootname(filename:basename(Filename))),
            Fn = if
                is_list(Src) -> filename:join([Src,Filename]);
                true -> Filename
            end,
            {Mod, Bin, Fn}
        end, FileList),
        {ok, Modules};
    Err ->
        Err
end.



code_load_modules(Modules) ->
    lists:foreach(fun({Mod, Bin, Fn})-> {module, _Mod} = code:load_binary(Mod, Fn, Bin) end, Modules),
    ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Compression
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode(Format, Data) ->
    case Format of
        term   -> {term, Data};
        binary -> {binary, erlang:term_to_binary(Data)};
        zip    -> {zip, erlang:term_to_binary(Data,compress)};
        snappy ->
            {ok, Bin} = snappy:compress(erlang:term_to_binary(Data)),
            {snappy, Bin}
    end.


decode(Format, Data) ->
    case Format of
        term   -> {ok, Data};
        binary -> {ok, erlang:binary_to_term(Data)};
        zip    -> {ok, erlang:binary_to_term(Data)};
        snappy -> {ok, Term} = snappy:decompress(Data)
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  CLI
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


resp(Resp) ->
    case Resp of
        {badrpc, Reason} -> die(Reason);
        {error, Reason} -> die(Reason);
        {warn, Msg} -> io:fwrite(standard_error, "warning: ~p~n", [Msg]), Resp;
        {info, Msg} -> io:format("info: ~p~n",[Msg]);
        {ok, Value} -> Value;
        _ -> Resp
    end.


getopts(OptSpecList0, Args, Required, MinInputs, MaxInputs, Prog, UsageText) ->
    OptSpecList = [{help, $h, "help", undefined, "display this help and exit" } | OptSpecList0],
    case getopt:parse(OptSpecList, Args) of
        {ok, {Opts, Input}} ->
            case lists:member(help, Opts) of
                true ->
                    getopt:usage(OptSpecList, "edfs "++Prog, UsageText),
                    halt(1);
                false -> ok
            end,
            % check required options
            lists:foreach(fun(Req)->
                case lists:keymember(Req, 1, Opts) of
                    false -> die("~p is required",[Req]);
                    _ -> ok
                end
            end, Required),
            % check input length
            Len = length(Input),
            if
                Len > MaxInputs -> die("too many arguments");
                Len < MinInputs -> die("not enough arguments");
                true -> {Opts, Input}
            end;
        {error, Reason} -> die(Reason)
    end.


die(Reason) ->
    io:fwrite(standard_error, "error: ~p~n", [Reason]),
    halt(1).

die(Format, Args) ->
    io:fwrite(standard_error, "error: "++Format++"~n", [Args]),
    halt(1).
