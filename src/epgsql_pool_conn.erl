%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool_conn).

-behaviour(gen_server).

-export([connection/1, release/1, close/1]).
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {pool, pid}).

connection(Name) -> gen_server:call(Name, connection, infinity).
release(Name) -> gen_server:call(Name, release, infinity).
start_link(Name) -> gen_server:start_link(?MODULE, Name, []).
close(Name) -> gen_server:call(Name, close, infinity).

init(Name) ->
    case epgsql_pool_config:conn_by_name(Name) of
        {error, not_found} ->
            {stop, {error, missing_configuration}};
        {ok, L} ->
            {ok, Pid} = apply(pgsql, connect, L),
            ok = epgsql_pool:available(Name, self()),
            {ok, #state{pool = Name, pid = Pid}}
    end.

terminate(shutdown, #state{pid = P}) -> pgsql:close(P).

handle_call(close, _From, State) ->
    {stop, shutdown, State};
handle_call(connection, _From, #state{pid = P} = State) ->
    {reply, {ok, P}, State};
handle_call(release, _From, #state{pool = Name} = State) ->
    ok = epgsql_pool:available(Name, self()),
    {reply, ok, State};
handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast(Msg, _) -> exit({unknown_cast, Msg}).
handle_info(Msg, _) -> exit({unknown_info, Msg}).
code_change(_OldVsn, State, _Extra) -> {ok, State}.
