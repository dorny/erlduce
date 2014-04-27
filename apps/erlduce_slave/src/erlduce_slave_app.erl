-module(erlduce_slave_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).


%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    application:load(edfs),
    application:load(erlduce),
    erlduce_sup:start_link().


stop(_State) ->
    ok.
