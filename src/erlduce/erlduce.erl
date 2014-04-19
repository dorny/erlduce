-module(erlduce).

-behaviour(application).


-export([
    start/0,
    start/2,
    stop/0,
    stop/1
]).


%% @doc Start the application and all dependencies
start() ->
    erlduce_utils:start_application(erlduce).


start(_StartType, _StartArgs) ->
    application:set_env(erlduce, master, node()),
    edfs:start(),
    {ok, Pid} = erlduce_sup:start_link().


stop() ->
    application:stop(erlduce).

stop(_State) ->
    ok.
