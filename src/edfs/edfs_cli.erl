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
        "  cp~n",
        "  link~n",
        "  ls~n",
        "  rm~n",
        "  stat~n",
        "  tag~n",
        "  unlink~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd("help", _Args) ->
    usage();


cmd("cp", Args) ->
    {ok, EdfsEnv} = application:get_env(erlduce, edfs),
    OptSpecList = [
        {block_size, $b, "block-size", {integer, proplists:get_value(block_size, EdfsEnv, 32)}, "size of a one block"},
        {type, $t, "type", {atom, text}, "file type, affects split method"},
        {replica, $r, "replicas", {integer, proplists:get_value(replicas, EdfsEnv, 3)}}
    ],
    {Opts, StrPaths} = erlduce_utils:getopts(OptSpecList, Args, [], 2, infinity, "cp", "[SOURCE ...] TAG\nCopy files into edfs storage"),
    Paths = [list_to_binary(P) || P <-StrPaths],
    {SrcPaths, [Dest]} = lists:split(length(Paths)-1, Paths),
    lists:foreach(fun(Src)->
        Resp = edfs:cp(Src, Dest, Opts),
        RespList = lists:flatten([Resp]),
        [erlduce_utils:resp(R) || R <- RespList]
    end, SrcPaths),
    halt();

cmd("format", _Args) ->
    application:load(erlduce),
    % Schema
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    MnesiaDir = filename:join(WorkDir, "mnesia"),
    os:cmd("mkdir -p \""++MnesiaDir++"\""),
    application:set_env(mnesia, dir, MnesiaDir),
    mnesia:delete_schema([node()]),
    % Blobs
    BlobsDir = filename:join(WorkDir, "blobs"),
    {ok, Nodes} = application:get_env(erlduce, nodes),
    [ os:cmd("ssh '"++atom_to_list(Host)++"' \"rm '"++BlobsDir++"'/*\"")  || {Host, _} <- Nodes],
    halt();

cmd("link", Args) ->
    OptSpecList = [
        {parent, undefined, undefined, binary, "tag path"},
        {child, undefined, undefined, binary, "tag path"}
    ],
    {Opts, _} = erlduce_utils:getopts(OptSpecList, Args, [], 0, 0, "link", "\nCreate symbolic link"),
    Parent = proplists:get_value(parent, Opts),
    Child = proplists:get_value(child, Opts),
    erlduce_utils:resp( edfs:link(Parent,Child)),
    halt();

cmd("ls", Args) ->
    OptSpecList = [{list, $l, undefined, undefined, "use a long listing format"}],
    {Opts, StrPaths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "ls", "[TAG ...]\nList TAG(s) children"),
    Paths = case StrPaths of
        [] -> [<<"/">>];
        _ -> [list_to_binary(P) || P <-StrPaths]
    end,
    ListOpt = lists:member(list, Opts),
    lists:foreach(fun(Path)->
        Listing = erlduce_utils:resp( edfs:ls(Path)),
        p_print_ls(Listing, ListOpt)
    end, Paths),
    halt();

cmd("rm", Args) ->
    OptSpecList = [{recursive, $r, "recursive", undefined, "remove tags and their contents recursively"}],
    {Opts, StrPaths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "rm", "[TAG ...]\nRemove (unlink) the TAG(s)."),
    Paths = [list_to_binary(P) || P <-StrPaths],
    Recursive = lists:member(recursive, Opts),
    [ erlduce_utils:resp( edfs:rm(Path, Recursive)) || Path <- Paths ],
    halt();

cmd("stat", Args) ->
    OptSpecList = [{path, undefined, undefined, binary, "tag path"}],
    {Opts, _} = erlduce_utils:getopts(OptSpecList, Args, [path], 0, 0, "tag","\nGet Tag stats"),
    Path = proplists:get_value(path, Opts),
    {C,B,S,T} = erlduce_utils:resp( edfs:stat(Path)),
    io:format("children: ~p~n",[C]),
    io:format("blobs: ~p~n",[B]),
    io:format("size: ~s~n",[erlduce_utils:format_size(S)]),
    io:format("total: ~s~n",[erlduce_utils:format_size(T)]),
    halt();

cmd("tag", Args) ->
    OptSpecList = [{path, undefined, undefined, binary, "tag path"}],
    {Opts, _} = erlduce_utils:getopts(OptSpecList, Args, [path], 0, 0, "tag","\nCreate new TAG"),
    Path = proplists:get_value(path, Opts),
    erlduce_utils:resp( edfs:tag(Path)),
    halt();

cmd("unlink", Args) ->
    OptSpecList = [
        {parent, undefined, undefined, binary, "tag path"},
        {child, undefined, undefined, binary, "tag path"}
    ],
    {Opts, _} = erlduce_utils:getopts(OptSpecList, Args, [], 0, 0, "unlink", "\nRemove symbolic link"),
    Parent = proplists:get_value(parent, Opts),
    Child = proplists:get_value(child, Opts),
    erlduce_utils:resp( edfs:unlink(Parent,Child)),
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
    lists:foreach(fun
        ({link, TagName}) -> io:format("{link, \"~s\"}~s",[TagName, Join]);
        (TagName)-> io:format("~s~s",[TagName, Join])
    end, Sorted),
    case Long of
        false -> io:format("~n");
        true -> ok
    end.


