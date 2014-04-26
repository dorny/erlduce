-module(edfs_slave_app).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(application).

-export([
    start/2,
    stop/1
]).


%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    edfs_slave_sup:start_link().


stop(_State) ->
    ok.
