-module(erlduce).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

-export([
    start/0,
    stop/0,
    run/3
]).


%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    erlduce_sup:start_link().


stop(_State) ->
    ok.


%% ===================================================================
%% API
%% ===================================================================

start() ->
    erlduce_utils:start_application(erlduce).


stop() ->
    application:stop(erlduce).


run(RunID,Opts,DriverArgs) when is_atom(RunID), is_list(Opts), is_list(DriverArgs) ->
    {ok, Resources} = application:get_env(erlduce,hosts),
    TarPath = proplists:get_value(tar, Opts),
    case erlduce_utils:tar_load_modules(TarPath) of
        {ok, Modules} ->
            erlduce_utils:code_load_modules(Modules),
            Mod = proplists:get_value(start, Opts),
            Hosts = [Host || {Host, _} <- Resources],
            {Slaves, Errors} = erlduce_utils:start_slaves(RunID,Hosts,[erlduce_slave]),
            todo;
        Error -> Error
    end.
