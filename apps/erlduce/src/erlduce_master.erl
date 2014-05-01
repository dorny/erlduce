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

-record(state, {
    job_counter = 0
}).

-define( ERLDUCE, {?MODULE, p_master_node()}).


start_link() ->
    gen_server:start_link({local, ?MODULE},?MODULE, {}, []).

run_slaves(RunID) ->
    gen_server:call(?ERLDUCE, {run_slaves, RunID}, infinity).

% %% ===================================================================
% %% GEN_SERVER
% %% ===================================================================

init(_Args) ->
    {ok, #state{}}.


handle_call( {run_slaves,StartName}, {Pid,_}, State=#state{ job_counter=JobCount }) ->
    RunID =  list_to_atom(atom_to_list(StartName)++"_"++integer_to_list(JobCount)),
    {ok, Resources} = application:get_env(erlduce,hosts),
    Slaves0 = erlduce_utils:start_slaves(RunID, Resources,[erlduce_slave],Pid),
    Slaves = lists:foldl(fun
        ({{_,Slots}, {ok,Node}}, Acc) -> [ {Node,Slots} | Acc];
        ({{Host,_}, {error,Reason}}, Acc) -> lager:warning("Failed to start wroker at ~p: ~p",[Host,Reason]), Acc
    end, [], Slaves0),

    case Slaves of
        [] -> {reply, {error, enoslots}, State};
        _  ->
            Resp = {ok, {RunID, Slaves}},
            {reply, Resp, State#state{ job_counter=JobCount+1 }}
    end;

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
