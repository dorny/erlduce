-module(erlduce_utils).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    start_application/1,
    host/0,
    host/1,
    node/2,

    pmap/2,
    pmap_first/2,
    pmap_first/3,
    pmap_first_wait/3,

    sem_new/1,
    sem_new/2,
    sem_loop/2,
    sem_wait/1,
    sem_signal/1,

    start_slaves/2,
    start_slaves/3,
    start_slave/4,

    tar_load_modules/1,
    code_load_modules/1,

    encode/1,
    encode/2,
    encode/3,
    decode/1,
    decode/2,

    getopts/7,
    cli_error/2,
    cli_die/2,
    format_size/1,
    parse_size/1
]).

-type node_list_ok() :: list({Host::atom(), Node::atom()}).
-type node_list_error() :: list({Host::atom(), Error::any()}).


%% @doc Start application
-spec start_application(App::atom()) -> ok.
start_application(App) ->
    start_ok(App, application:start(App, permanent)).
start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, {not_started, Dep}}) ->
    ok = start_application(Dep),
    start_application(App);
start_ok(App, {error, Reason}) ->
    erlang:error({app_start_failed, App, Reason}).



%% @doc Get localhost name
-spec host() -> atom().
host() ->
    host(node()).

%% @doc Get hostname from nodename
-spec host(Node::atom()) -> atom().
host(Node) when is_atom(Node) ->
    list_to_atom(after_char($@, atom_to_list(Node))).

%% @doc Get nodename from name and host
-spec node(Name::atom(), Host::atom()) -> atom().
node(Name, Host) when is_atom(Name), is_atom(Host) ->
    list_to_atom(atom_to_list(Name)++"@"++atom_to_list(Host)).

after_char(_, []) -> [];
after_char(Char, [Char|Rest]) -> Rest;
after_char(Char, [_|Rest]) -> after_char(Char, Rest).


%% @doc Parallel map on local node
-spec pmap(F::function(), L::list()) -> list().
pmap(F, L) ->
    Parent = self(),
    [ receive {Pid, Result} -> Result end || Pid <- [spawn_link(fun() -> Parent ! {self(), F(X)} end) || X <- L]].

%% @doc Parallel map on local node. Returns immediatelly after first function returns anything.
-spec pmap_first(F::function(), L::list()) -> any().
pmap_first(F, L) ->
    pmap_first(F,L, '_').

%% @doc Parallel map on local node.
%%      Returns {ok, Result} when first function return M=Result or M(Result)=true, otherwise it returns false
-spec pmap_first(F::function(), L::list(), M::function()|term()) -> any().
pmap_first(F, L, M) ->
    Master = spawn_link(?MODULE, pmap_first_wait, [length(L), M, self()]),
    [spawn_link(fun() -> Master ! F(X) end) || X <- L],
    receive
        {Master, Result} -> Result
    end.
%% @private
pmap_first_wait(0, _, Parent) -> Parent ! {self(), false};
pmap_first_wait(Len, M, Parent)->
    receive
        M -> Parent ! {self(), {ok, M}};
        Result when is_function(M) ->
            case M(Result) of
                true -> Parent ! {self(), {ok,Result}};
                _ -> pmap_first_wait(Len -1, M, Parent)
            end;
        Result when M=:='_' -> Parent ! {self(), {ok, Result}};
        _ -> pmap_first_wait(Len -1, M, Parent)
    end.

%% @doc Spawns new Semaphore
-spec sem_new(Full::number()) -> pid().
sem_new(Full) ->
    sem_new(0,Full).

-spec sem_new(Init::number(), Full::number()) -> pid().
sem_new(Init, Full) when is_number(Init); is_number(Full); Init>=0; Full>0 ->
    spawn(?MODULE, sem_loop, [Init, Full]).

%% @private
sem_loop(N, F) when N < F ->
    receive
        {wait, Pid} ->
            Pid ! {self(), ok},
                sem_loop(N+1,F);

        {signal, Pid} when N>0->
            Pid ! {self(), ok},
            sem_loop(N-1, F);

        {signal, Pid} ->
            Pid ! {self(), {error, badarg}},
            sem_loop(N, F)
    end;
sem_loop(N, F) ->
    receive
        {signal, Pid} ->
            Pid ! {self(), ok},
            sem_loop(N-1, F)
    end.



%% @doc Increment semaphore
-spec sem_wait(Pid::pid()) -> ok.
sem_wait(Pid) ->
    Pid ! {wait, self()},
    receive
        {Pid, ok} -> ok
    end.


%% @doc Decrement semaphore
-spec sem_signal(Pid::pid()) -> ok.
sem_signal(Pid) ->
    Pid ! {signal, self()},
    receive
        {Pid, Resp} -> Resp
    end.



%% @doc Start salves with Name on each Host in parallel.
-spec start_slaves(Name::atom(), Hosts::list(atom())) ->
    {node_list_ok(), node_list_error()}.
start_slaves(Name, Hosts) ->
    start_slaves(Name, Hosts, []).

%% @doc Start salves with Name on each Host in parallel.
%%      Each Application will be run with env variables from master node.
-spec start_slaves(Name::atom(), Hosts::list(atom()), Applications::list(atom())) ->
    {node_list_ok(), node_list_error()}.
start_slaves(Name, Hosts, Applications) ->
    Master = self(),
    Slaves = pmap(fun(Host)->
        {Host, start_slave(Name,Host,Master, Applications)}
    end, Hosts),
    lists:foldl(fun
        ({Host, {ok, Node}}, {Ok,Err}) ->
            {[Node|Ok], Err};
        ({Host, Error}, {Ok,Err}) ->
            {Ok, [{Host, Error}|Err]}
    end, {[],[]}, Slaves).

%% Start linked slave with name and start applications.
-spec start_slave(Name::atom(), Host::atom(), LinkTo::pid(), Applications::list(atom())) ->
    {ok, Node::atom()} | {error, Reason::any()}.
start_slave(Name, Host, LinkTo, Applications) ->
    Paths = lists:foldr(fun(P, Acc)-> lists:concat([Acc," \"",filename:absname(P),"\""]) end, "", code:get_path()),

    {ok,[[Progname]]} = init:get_argument(progname),

    Opts = case init:get_argument(config) of
        {ok,[[Config]]} -> [" -config \"", Config, "\""];
        _ -> []
    end,
    Res = slave:start(Host, Name, lists:concat([
        " -setcookie ", atom_to_list(erlang:get_cookie()),
        " -pa ", Paths
        | Opts
    ]), LinkTo, Progname),

    case Res of
        {ok, Node} ->
            [rpc:call(Node, ?MODULE, start_application, [App]) || App <- Applications],
            Res;
        Error ->
            Error
    end.


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


encode(Term) ->
    encode(Term, none).
encode(Bytes, Compress) when is_binary(Bytes) ->
    encode(binary, Bytes, Compress);
encode(Term, Compress) ->
    encode(term, term_to_binary(Term), Compress).
encode(Format, Bytes, none) ->
    {Format, Bytes};
encode(Format, Bytes, zip) ->
    {{zip,Format}, zlib:compress(Bytes)};
encode(Format, Bytes, snappy) ->
    {ok, Compressed} = snappy:compress(Bytes),
    {{snappy, Format}, Compressed}.


decode({Spec, Data}) ->
    decode(Spec, Data).
decode(binary, Bytes) ->
    Bytes;
decode(term, Bytes) ->
    binary_to_term(Bytes);
decode({none, Format}, Bytes) ->
    decode(Format,Bytes);
decode({zip, Format}, Bytes) ->
    decode(Format, zlib:uncompress(Bytes));
decode({snappy, Format}, Bytes) ->
    {ok, Bytes2} = snappy:decompress(Bytes),
    decode(Format,Bytes2).




getopts(OptSpecList0, Args, Required, MinInputs, MaxInputs, Prog, UsageText) ->
    OptSpecList = [{help, $h, "help", undefined, "display this help and exit" } | OptSpecList0],
    case getopt:parse(OptSpecList, Args) of
        {ok, {Opts, Input}} ->
            case lists:member(help, Opts) of
                true ->
                    getopt:usage(OptSpecList, Prog, UsageText),
                    halt(1);
                false -> ok
            end,
            % check required options
            lists:foreach(fun(Req)->
                case lists:keymember(Req, 1, Opts) of
                    false -> cli_die("~p is required",[Req]);
                    _ -> ok
                end
            end, Required),
            % check input length
            Len = length(Input),
            if
                Len > MaxInputs -> cli_die("too many arguments",[]);
                Len < MinInputs -> cli_die("not enough arguments",[]);
                true -> {Opts, Input}
            end;
        {error, Reason} -> cli_die("~p",[Reason])
    end.

cli_error(Format,Args) ->
    io:fwrite(standard_error, "error: "++Format++"~n", Args).

cli_die(Format, Args) ->
    cli_error(Format,Args),
    halt(1).


format_size(S) when S >= 1024*1024*1024 -> p_format_size(S, 1024*1024*1024, "GB");
format_size(S) when S >= 1024*1024 -> p_format_size(S, 1024*1024, "MB");
format_size(S) when S >= 1024 -> p_format_size(S, 1024, "KB");
format_size(S) -> integer_to_list(S)++" B".

p_format_size(S, U, Unit) ->
    MB = S/U, N = trunc(MB), D = round((MB-N)*10),
    if
        D>=10 -> integer_to_list(N+1)++" "++Unit;
        D>0 -> integer_to_list(N)++"."++integer_to_list(D)++" "++Unit;
        true -> integer_to_list(N)++" "++Unit
    end.


parse_size(S) when is_integer(S) -> S;
parse_size(S) when is_list(S) ->
    {N, Rest} = string:to_integer(S),
    case string:strip(Rest, both) of
        "" -> N;
        "B" -> N;
        "KB" -> N*1024;
        "MB" -> N*1024*1024;
        "GB" ->N*1024*1024*1024
    end.
