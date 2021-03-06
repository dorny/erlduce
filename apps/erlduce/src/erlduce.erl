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


run(Start, Modules, DriverArgs) when is_atom(Start), is_list(Modules), is_list(DriverArgs) ->
    application:load(erlduce_slave),
    case erlduce_master:run_slaves(Start) of
        {ok, {RunID, Slaves}} ->
            erlduce_utils:pmap(fun({Node,_})->
                rpc:call(Node, erlduce_utils, code_load_modules, [Modules])
            end, Slaves),
            erlduce_utils:code_load_modules(Modules),
            Start:start(RunID,Slaves,DriverArgs);
        Error-> Error
    end.


