-module(erlduce_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    cmd/0,
    cmd_help/1,
    cmd_stop/1,
    connect/0,
    ensure_connect/0
]).


cmd() ->
    application:load(erlduce),
    Args = init:get_plain_arguments(),
    case Args of
        ["fs" | TArgs] -> edfs_cli:cmd(TArgs);
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
        "  start~n"
        "  stop~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd_stop(Args) ->
    erlduce_utils:getopts([], Args, [], 0, 0, "stop", "\nStop erlduce node"),
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

