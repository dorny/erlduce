-module(edfs_cli).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    cmd/1,
    cmd_help/1,
    cmd_cat/1,
    cmd_format/1,
    cmd_import/1,
    cmd_ls/1,
    cmd_mkdir/1,
    cmd_mkfile/1,
    cmd_rm/1
]).

-include("edfs.hrl").


cmd(Args) ->
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
        "Usage: edfs <command> [options ...] [args ...]~n~n",
        "Commands are:~n",
        "  cat~n",
        "  import~n",
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
    {Opts, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 1, infinity, "edfs cat",
        "[FILE ...]\nWrite FILE(s) to standard output"),
    Extract = proplists:get_value(extract, Opts),
    erlduce_cli:ensure_connect(),
    [
        case edfs:cat(Path,standard_io,Extract) of
            ok -> ok;
            {error, Reason} ->
                erlduce_utils:cli_die("~p",[Reason])
        end
    || Path <- Paths].


cmd_format(Args) ->
    erlduce_utils:getopts([], Args, [], 0, 0, "edfs format","\nFormat EDFS storage"),
    erlduce_cli:ensure_connect(),
    edfs:format().


cmd_import(Args) ->
    {ok, EnvBlockSize} = application:get_env(edfs, block_size),
    {ok, DefaultReplicas} = application:get_env(edfs, replicas),
    DefaultBlockSize = erlduce_utils:parse_size(EnvBlockSize),
    OptSpecList = [
        {block_size, $b, "block-size", {string, DefaultBlockSize}, "size of a one block"},
        {compress, $c, "compress", {atom,none}, "use compression: none, zip, snappy"},
        {replica, $r, "replicas", {integer, DefaultReplicas}, "number of replicas"},
        {type, $t, "type", {atom, text}, "file type: text, binary, lines"}
    ],
    {Opts0, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 2, infinity,"edfs import",
        "[SOURCE ...] DESTINATION\nImport files and directories into edfs storage"),
    BlockSize = erlduce_utils:parse_size(proplists:get_value(block_size, Opts0)),
    Opts = lists:keyreplace(block_size, 1, Opts0, {block_size, BlockSize}),
    {SrcPaths, [Dest]} = lists:split(length(Paths)-1, Paths),
    erlduce_cli:ensure_connect(),
    [ p_import(Src,Dest, Opts) || Src <- SrcPaths ],
    ok.


cmd_ls(Args) ->
    OptSpecList = [{list, $l, undefined, undefined, "use a long listing format"}],
    {Opts, Paths} = erlduce_utils:getopts(OptSpecList, Args, [], 0, infinity, "edfs ls",
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
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "edfs mkdir",
        "[DIRECTORY ...]\nCreate new DIRECTORY(ies)"),
    erlduce_cli:ensure_connect(),
    [ case edfs:mkdir(Path) of
        {ok, _Inode} -> ok;
        Error -> {error, {Path,Error}}
    end || Path <- Paths].


cmd_mkfile(Args) ->
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "edfs mkfile", "[FILE ...]\nCreate new FILE(s)"),
    erlduce_cli:ensure_connect(),
    [ case edfs:mkfile(Path) of
        {ok, _Inode} -> ok;
        Error -> {error, {Path,Error}}
    end || Path <- Paths].


cmd_rm(Args) ->
    {_, Paths} = erlduce_utils:getopts([], Args, [], 1, infinity, "edfs rm", "[FILE ...]\nRemove FILE(s)"),
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


p_import(Src, Dest, Opts) ->
    case filelib:is_file(Src) of
        false ->
            erlduce_utils:cli_die("No such file or directory: ~p",[Src]);
        true ->
            case edfs:stat(Dest) of
                {ok, {_, directory}} ->
                    Dest2 = filename:join(Dest, lists:last(filename:split(Src))),
                    p_import2(Src, Dest2, Opts);
                {ok, {_, regular}} ->
                    erlduce_utils:cli_die("Cannot override existing file: ~p",[Dest]);
                {error, enoent} ->
                    p_import2(Src, Dest, Opts)
            end
    end.
p_import2(Src, Dest, Opts) ->
    case filelib:is_dir(Src) of
        true -> p_import_dir(Src,Dest, Opts);
        false -> p_import_file(Src,Dest, Opts)
    end.
p_import_dir(Src, Dest, Opts) ->
    case edfs:mkdir(Dest) of
        {ok, _Inode} ->
            case file:list_dir(Src) of
                {ok, Filenames} ->
                    [p_import2(filename:join(Src,File), filename:join(Dest,File), Opts) || File <- Filenames],
                    ok;
                {error, Reason} ->
                    erlduce_utils:cli_error("~p: ~p",[Src, Reason])
            end;
        {error,Reason} -> erlduce_utils:cli_error("~p: ~p",[Dest, Reason])
    end.
p_import_file(Src,Dest, Opts) ->
    Replicas = proplists:get_value(replicas, Opts, 3),
    case edfs:mkfile(Dest) of
        {ok, Inode} ->
            edfs_lib:read_file(Src, Opts, fun
                (eof) -> ok;
                (Bytes) ->
                    case edfs:write(Inode, Bytes, Replicas) of
                        ok -> ok;
                        {error, Reason} -> erlduce_utils:cli_error("~p: ~p",[Dest, Reason])
                    end
            end);
        {error,Reason} -> erlduce_utils:cli_error("~p: ~p",[Dest, Reason])
    end.
