-module(edfs_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    cmd/1,
    cmd_help/1,
    cmd_cat/1,
    cmd_cp/1,
    cmd_format/1,
    cmd_ls/1,
    cmd_mkdir/1,
    cmd_mkfile/1,
    cmd_rm/1
]).



cmd(Args) ->
    application:load(edfs),
    Resp = case Args of
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
    Status = case erlduce_utils:resp(Resp) of
        ok -> 0;
        error -> 1
    end,
    halt(Status).


cmd_help(_Args) ->
    io:fwrite(standard_error, lists:concat([
        "Usage: edfs <command> [options ...] [args ...]~n~n",
        "Commands are:~n",
        "  cat~n",
        "  cp~n",
        "  format~n",
        "  ls~n",
        "  mkdir~n",
        "  mkfile~n",
        "  rm~n",
        "  stop~n",
        "  help~n",
        "~n"
    ]), []),
    halt(1).


cmd_cat(Args) ->
    OptSpecList = [
        {extract, $x, "extract", {atom,none}, "decompress stored data: none,zip,snappy"}
    ],
    {Opts, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 1, infinity, "cat",
        "[FILE ...]\nWrite FILE(s) to standard output"),
    Extract = proplists:get_value(extract, Opts),
    erlduce_cli:ensure_connect(),
    [ edfs:cat(Path,standard_io,Extract) || Path <- Paths].


cmd_cp(Args) ->
    {ok, EnvBlockSize} = application:get_env(edfs, block_size),
    {ok, DefaultReplicas} = application:get_env(edfs, replicas),
    DefaultBlockSize = erlduce_utils:parse_size(EnvBlockSize),
    OptSpecList = [
        {block_size, $b, "block-size", {string, DefaultBlockSize}, "size of a one block"},
        {compress, $c, "compress", {atom,none}, "use compression: none, zip, snappy"},
        {replica, $r, "replicas", {integer, DefaultReplicas}, "number of replicas"},
        {type, $t, "type", {atom, text}, "file type, affects split method"}
    ],
    {Opts0, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 2, infinity,"cp",
        "[SOURCE ...] DESTINATION\nCopy files and directories into edfs storage"),
    BlockSize = erlduce_utils:parse_size(proplists:get_value(block_size, Opts0)),
    Opts = lists:keyreplace(block_size, 1, Opts0, {block_size, BlockSize}),
    {SrcPaths, [Dest]} = lists:split(length(Paths)-1, Paths),
    erlduce_cli:ensure_connect(),
    [ edfs:cp(Src,Dest, Opts) || Src <- SrcPaths ].


cmd_format(Args) ->
    erlduce_utils:getopts([], Args, [], 0, 0, "format","\nFormat EDFS storage"),
    erlduce_cli:ensure_connect(),
    edfs:format().


cmd_ls(Args) ->
    OptSpecList = [{list, $l, undefined, undefined, "use a long listing format"}],
    {Opts, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "ls",
        "[DIRECTORY ...]\nList files in DIRECTORY(ies)"),
    ListOpt = lists:member(list, Opts),
    Paths2 = case Paths of
        [] -> ["/"];
        _ -> Paths
    end,
    erlduce_cli:ensure_connect(),
    [ case edfs:ls(Path) of
        {ok, Listing} ->
            p_print_ls(Listing, ListOpt),
            ok;
        Error -> {error, {Path,Error}}
    end || Path <- Paths2].


cmd_mkdir(Args) ->
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "mkdir",
        "[DIRECTORY ...]\nCreate new DIRECTORY(ies)"),
    erlduce_cli:ensure_connect(),
    [ case edfs:mkdir(Path) of
        {ok, _Inode} -> ok;
        Error -> {error, {Path,Error}}
    end || Path <- Paths].


cmd_mkfile(Args) ->
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "mkfile", "[FILE ...]\nCreate new FILE(s)"),
    erlduce_cli:ensure_connect(),
    [ case edfs:mkfile(Path) of
        {ok, _Inode} -> ok;
        Error -> {error, {Path,Error}}
    end || Path <- Paths].


cmd_rm(Args) ->
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "rm", "[FILE ...]\nRemove FILE(s)"),
    erlduce_cli:ensure_connect(),
    [ edfs:rm(Path) || Path <- Paths ].


%% ===================================================================
%% PRIVATE
%% ===================================================================

p_print_ls([], _) ->
    ok;
p_print_ls(Listing, Long) ->
    Names = [ Name || {Name, _Inode} <- Listing],
    Sorted = lists:sort(Names),
    Join = case Long of
        true -> "\n";
        false -> " "
    end,
    [ io:format("~s~s",[Name, Join]) || Name <- Sorted],
    case Long of
        false -> io:format("~n"), ok;
        true -> ok
    end.
