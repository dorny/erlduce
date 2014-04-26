-module(erlduce_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    Node = node(),
    Args = case application:get_env(erlduce,master) of
        undefined ->
            application:set_env(erlduce,master,Node),
            master;
        {ok, Node} -> master;
        {ok, _OtherNode} -> slave
    end,
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(master) ->
    {ok, { {one_for_one, 5, 10}, []}}.

