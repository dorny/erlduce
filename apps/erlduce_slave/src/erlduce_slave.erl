-module(erlduce_slave).

-author("Michal Dorner <dorner.michal@gmail.com>").

-behaviour(gen_server).

-export([
    start_link/3
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
    id :: integer(),
    master::pid(),

    input :: function(),
    map :: function(),
    combine :: function(),
    reduce :: function(),
    output :: function()
}).


start_link(Master, {Node, SlaveID}, JobSpec) ->
    rpc:call(Node, gen_server, start, [?MODULE, {Master, SlaveID, JobSpec}, []]).


%% ===================================================================
%% GEN_SERVER
%% ===================================================================

init({Master, SlaveID, JobSpec}) ->
    link(Master),
    {ok, #state{
        id = SlaveID,
        master = Master,
        input = proplists:get_value(input, JobSpec, undefined),
        map = proplists:get_value(map, JobSpec, undefined),
        combine = proplists:get_value(combine, JobSpec, undefined),
        reduce = proplists:get_value(reduce, JobSpec, undefined),
        output = proplists:get_value(output, JobSpec, undefined)
    }}.

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
