%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool_conn).

-behaviour(gen_server).

-export([connection/1, return/1]).
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {pool, pid, available = true}).

connection(Name) -> gen_server:call(Name, connection, infinity).
return(Name) -> gen_server:call(Name, return, infinity).
start_link(Name) -> gen_server:start_link(?MODULE, Name, []).

init(Name) ->
    case epgsql_pool_config:conn_by_name(Name) of
        {error, not_found} ->
            {stop, {error, missing_configuration}};
        {ok, L} ->
            {ok, Pid} = apply(pgsql, connect, L),
            ok = epgsql_pool:available(Name, self()),
            {ok, #state{pool = Name, pid = Pid}}
    end.

terminate(shutdown, _) -> ok.

handle_call(connection, _From, #state{pid = P} = State) ->
    {reply, {ok, P}, State#state{available = false}};
handle_call(return, _From, #state{available = false, pool = Name} = State) ->
    ok = epgsql_pool:available(Name, self()),
    {reply, ok, State#state{available = true}};
handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast(Msg, _) -> exit({unknown_cast, Msg}).
handle_info(Msg, _) -> exit({unknown_info, Msg}).
code_change(_OldVsn, State, _Extra) -> {ok, State}.
