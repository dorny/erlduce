-module(edfs_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start/0,
    cmd/2
]).


start() ->
    application:load(erlduce).


usage() ->
    io:fwrite(standard_error, lists:concat([
        "Usage: edfs <command> [options ...] [args ...]~n~n",
        "Commands are:~n",
        "  clear~n",
        "  cp~n",
        "  ls~n",
        "  rm~n",
        "  stat~n",
        "  tag~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd("help", _Args) ->
    usage();

cmd("clear", _Args) ->
    erlduce_utils:resp(edfs:clear()),
    halt();

% cmd("cp", Args) ->
    % todo;

cmd("format", _Args) ->
    application:load(erlduce),
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    MnesiaDir = filename:join(WorkDir, "mnesia"),
    os:cmd("mkdir -p \""++MnesiaDir++"\""),
    application:set_env(mnesia, dir, MnesiaDir),
    mnesia:delete_schema([node()]),
    halt();

% cmd("link", Args) ->
    % todo;

cmd("ls", Args) ->
    OptSpecList = [{list, $l, undefined, undefined, "use a long listing format"}],
    {Opts, StrPaths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "ls", "[paths ...]"),
    Paths = [list_to_binary(P) || P <-StrPaths],
    ListOpt = lists:member(list, Opts),
    lists:foreach(fun(Path)->
        % Listing = edfs_rpc(ls, [Path]),
        Listing = erlduce_utils:resp( edfs:ls(Path)),
        p_print_ls(Listing, ListOpt)
    end, Paths),
    halt();

cmd("rm", Args) ->
    OptSpecList = [{recursive, $r, "recursive", undefined, "remove tags and their contents recursively"}],
    {Opts, StrPaths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "rm", "[paths ...]"),
    Paths = [list_to_binary(P) || P <-StrPaths],
    Recursive = lists:member(recursive, Opts),
    lists:foreach(fun(Path)->
        erlduce_utils:resp( edfs:rm(Path, Recursive))
    end, Paths),
    halt();


cmd("stat", _Args) ->
    todo;

cmd("tag", Args) ->
    OptSpecList = [{path, undefined, undefined, binary, "tag path"}],
    {Opts, _} = erlduce_utils:getopts(OptSpecList, Args, [path], 0, 0, "tag",""),
    Path = proplists:get_value(path, Opts),
    erlduce_utils:resp( edfs:tag(Path)),
    halt();

cmd(Cmd, _Args) ->
    io:fwrite(standard_error, "Unknown command: ~s~n", [Cmd]),
    usage().



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  PRIVATE
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

p_print_ls([], _) ->
    ok;
p_print_ls(Listing, Long) ->
    Join = case Long of
        true -> "\n";
        false -> " "
    end,
    Sorted = lists:sort(Listing),
    lists:foreach(fun(TagName)-> io:format("~s~s",[TagName, Join]) end, Sorted),
    case Long of
        false -> io:format("~n");
        true -> ok
    end.


