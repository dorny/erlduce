-module(erlduce).

-behaviour(application).


-export([
    start/0,
    start/2,
    stop/0,
    stop/1
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
