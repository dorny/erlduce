-module(erlduce_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    cmd/0,
    cmd_help/1,
    cmd_dist/1,
    cmd_fs/1,
    cmd_run/1,
    cmd_stop/1,
    connect/0,
    ensure_connect/0
]).


cmd() ->
    application:load(erlduce),
    application:load(edfs),
    application:load(mnesia),
    Args = init:get_plain_arguments(),
    case Args of
        [StrCmd | TArgs] ->
            Cmd = list_to_atom("cmd_"++StrCmd),
            case erlang:function_exported(?MODULE, Cmd, 1) of
                true -> ?MODULE:Cmd(TArgs);
                false ->
                    io:fwrite(standard_error, "Unknown command: ~s~n", [Cmd]),
                    cmd_help(Args)
            end;
        [] -> cmd_help(Args)
    end,
    halt().


cmd_help(_Args) ->
    io:fwrite(standard_error, lists:concat([
        "Usage: erlduce <command> [options ...] [args ...]~n~n",
        "Commands are:~n",
        "  dist~n",
        "  fs~n",
        "  run~n",
        "  start~n"
        "  stop~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd_dist(Args) ->
    {_, Files0} = erlduce_utils:getopts([], Args, [], 0, infinity, "erlduce dist", "Distribute files across cluster"),
    Files1 = lists:foldl(fun(File, Acc)->
        Abs = filename:absname(File),
        case file:read_file(File) of
            {ok,Bytes} -> [{file, Abs, Bytes}|Acc];
            {error, Reason} ->
                io:fwrite(standard_error,"error: ~p: ~p~n",[File,Reason]),
                Acc
        end
    end, [], Files0),
    Files = lists:foldl(fun(App, Acc)->
        {ok, Dir} = application:get_env(App, dir),
        [{directory, filename:absname(Dir)}|Acc]
    end, Files1, [erlduce,edfs,mnesia]),

    Localhost = erlduce_utils:host(),
    {ok, Resources} = application:get_env(erlduce,hosts),
    Hosts = [Host || {Host, _} <- Resources, Host=/=Localhost],
    Slaves0 = erlduce_utils:pmap(fun(Host)->
        slave:start(Host, 'erlduce_dist', " -setcookie "++atom_to_list(erlang:get_cookie()))
    end, Hosts),
    Slaves=lists:foldl(fun
        ({ok, Node},Acc)-> [Node|Acc];
        ({error,Reason},Acc) -> io:fwrite(standard_error,"error: ~p",[Reason]), Acc
    end, [], Slaves0),
    erlduce_utils:pmap(fun(Node)->
        lists:foreach(fun
            ({directory,Path}) -> rpc:call(Node, os, cmd, ["mkdir -p '"++Path++"'"]);
            ({file,Path,Bytes})->
                rpc:call(Node, os, cmd, ["mkdir -p '"++filename:dirname(Path)++"'"]),
                case rpc:call(Node, prim_file, write_file, [Path,Bytes]) of
                    ok -> ok;
                    {error, Reason} ->
                        io:fwrite(standard_error,"error: ~p[~p]: ~p~n",[Path,Node,Reason])
                end
        end, Files)
    end, Slaves),
    halt(0).


cmd_fs(Args) ->
    edfs_cli:cmd(Args).


cmd_run(Args) ->
    OptSpecList = [
        {tar, $t, "tar", string, "Path to tar file with compiled sources"},
        {pa, $p, "path", string, "Path to directory with compiled sources"},
        {start, $s, "start", atom, "Name of a main module"}
    ],
    {Opts, DriverArgs} = erlduce_utils:getopts(OptSpecList, Args, [start], 0, infinity, "erlduce run","Run erlduce driver"),
    Start = proplists:get_value(start, Opts),

    PaModules = lists:flatmap(fun(Path)->
        case erlduce_utils:path_load_modules(Path) of
            {ok, Modules} -> Modules;
            {error,Reason} -> io:fwrite(standard_error,"error: ~p: ~p~n",[Path,Reason]), halt(1)
        end
    end, proplists:get_all_values(pa, Opts)),
    TarModules = lists:flatmap(fun(Path)->
        case erlduce_utils:tar_load_modules(Path) of
            {ok, Modules} -> Modules;
            {error,Reason} -> io:fwrite(standard_error,"error: ~p: ~p~n",[Path,Reason]), halt(1)
        end
    end, proplists:get_all_values(tar, Opts)),
    Modules = PaModules++TarModules,
    case erlduce:run(Start, Modules, DriverArgs) of
        ok -> halt();
        {ok, Result} -> io:format("~p~n",[Result]), halt();
        {error, Reason} -> io:fwrite(standard_error, "error: ~p~n", [Reason]), halt(1)
    end.


cmd_stop(Args) ->
    erlduce_utils:getopts([], Args, [], 0, 0, "erlduce stop", "\nStop erlduce node"),
    {ok, Master} = erlduce_cli:ensure_connect(),
    case rpc:call(Master, init, stop, []) of
        ok -> halt(0);
        Error ->
            io:fwrite(standard_error, "~p",[Error]),
            halt(1)
    end.


connect() ->
    Master = case application:get_env(erlduce, master) of
        {ok, Node} -> Node;
        _ ->
            io:fwrite(standard_error, "error: erlduce master is not set in config.~n",[]),
            halt(1)
    end,
    case net_kernel:connect_node(Master) of
        true -> {ok, Master};
        Error -> Error
    end.

ensure_connect() ->
    case connect() of
        {ok, Master} -> {ok, Master};
        _ ->
            io:fwrite(standard_error, "erlduce is not running~n",[]),
            halt(1)
    end.


%% ===================================================================
%% PRIVATE
%% ===================================================================
