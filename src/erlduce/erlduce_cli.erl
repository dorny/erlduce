-module(erlduce_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    cmd/2
]).


start() ->
    application:load(erlduce).


usage() ->
    io:fwrite(standard_error, lists:concat([
        "Usage: erlduce <command> [options ...] [args ...]~n~n",
        "Commands are:~n",
        "  stop~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd("help", _Args) ->
    usage();

cmd("stop", _Args) ->
    erlduce_utils:run_at_master(init, stop, []),
    halt();

cmd(Cmd, _Args) ->
    io:fwrite(standard_error, "Unknown command: ~s~n", [Cmd]),
    usage().
