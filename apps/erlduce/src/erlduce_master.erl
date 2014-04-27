-module(erlduce_master).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/0,
    run_slaves/1
]).


%% gen_server.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define( ERLDUCE, {?MODULE, p_master_node()}).


start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).

run_slaves(RunID) ->
    gen_server:call(?ERLDUCE, {run_slaves, RunID}).

% %% ===================================================================
% %% GEN_SERVER
% %% ===================================================================

init(_Args) ->
    {ok, undefined}.


handle_call( {run_slaves,RunID}, {Pid,_}, State) ->
    {ok, Resources} = application:get_env(erlduce,hosts),
    Slaves0 = erlduce_utils:start_slaves(RunID, Resources,[erlduce_slave],Pid),
    Slaves = lists:foldl(fun
        ({{_,Slots}, {ok,Node}}, Acc) -> [ {Node,Slots} | Acc];
        ({{Host,_}, {error,Reason}}, Acc) -> lager:warning("Failed to start wroker at ~p: ~p",[Host,Reason]), Acc
    end, [], Slaves0),
    {reply,{ok,Slaves}, State};

handle_call( _Request, _From, State) ->
    {reply, ignored, State}.


handle_cast( _Msg, State) ->
    {noreply, State}.


handle_info( _Info, State) ->
    {noreply, State}.


terminate( _Reason,_State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% PRIVATE
%% ===================================================================


%% ===================================================================
%% UTILS
%% ===================================================================

p_master_node() ->
    case application:get_env(erlduce,master) of
        {ok, Node} -> Node;
        undefined -> exit({error,{env_not_set,{erlduce,master}}})
    end.
