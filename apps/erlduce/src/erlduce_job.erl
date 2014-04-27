-module(erlduce_job).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

% public api
-export([
    start_link/2,
    wait/1
]).

% internal mapred
-export([
    get_input/1
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
    slaves :: list(pid()),
    wait = [] :: list(pid())
}).


start_link(Nodes, JobSpec) ->
    gen_server:start_link(?MODULE, {Nodes, JobSpec}, []).


wait(Pid) ->
    gen_server:call(Pid, wait, infinity).


get_input(Pid) ->
    gen_server:call(Pid, get_input, infinity).

%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init({Nodes, JobSpec}) ->
    {Slaves,_} = lists:foldl(fun({Node,Slots},{Slaves, Sum})->
        Slaves2 = lists:foldl(fun(S,Acc)-> [{Node,Sum+S} | Acc]  end, Slaves, lists:seq(1, Slots)),
        {Slaves2, Sum+Slots}
    end, {[],0}, Nodes),

    Master = self(),
    SlavePids = erlduce_utils:pmap(fun(SlaveSpec)->
        {ok, Pid} = erlduce_slave:start_link(Master, SlaveSpec, JobSpec), Pid
    end, Slaves),

    {ok, #state{
        slaves=SlavePids
    }}.

handle_call( wait, From, State=#state{wait=Wait}) ->
    {noreply, State#state{ wait=[From|Wait] }};

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
