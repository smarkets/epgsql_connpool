%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool_conn).

-behaviour(gen_server).

-export([connection/1, release/1, close/1, transaction/4, dirty/4]).
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {pool, pid}).

connection(Name) -> gen_server:call(Name, connection, infinity).
release(Name) -> gen_server:call(Name, release, infinity).
start_link(Name) -> gen_server:start_link(?MODULE, Name, []).
close(Name) -> gen_server:call(Name, close, infinity).
transaction(Name, F, Args, Timeout) ->
    try gen_server:call(Name, {transaction, F, Args}, Timeout)
    catch
        exit:{timeout, _} ->
            exit(Name, timeout),
            receive
                {Mref, Reply} when is_reference(Mref) -> Reply
            after 0 -> {error, transaction_timeout}
            end
    end.
dirty(Name, F, Args, Timeout) ->
    try gen_server:call(Name, {dirty, F, Args}, Timeout)
    catch
        exit:{timeout, _} ->
            exit(Name, timeout),
            receive
                {Mref, Reply} when is_reference(Mref) -> Reply
            after 0 -> {error, transaction_timeout}
            end
    end.

init(Name) ->
    case epgsql_connpool_config:conn_by_name(Name) of
        {error, not_found} ->
            {stop, {error, missing_configuration}};
        {ok, L} ->
            case apply(pgsql, connect, L) of
                {ok, Pid} ->
                    ok = epgsql_connpool:available(Name, self()),
                    {ok, #state{pool = Name, pid = Pid}};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

terminate(shutdown, #state{pid = P}) -> pgsql:close(P).

handle_call({transaction, F, Args0}, _From, #state{pid = P} = State) ->
    try {ok, [], []} = pgsql:squery(P, "BEGIN"),
         R = apply(F, [P|Args0]),
         {ok, [], []} = pgsql:squery(P, "COMMIT"),
         %% XXX: What if exit signal is received here? Response may be lost.
         {reply, {ok, R}, State}
    catch
        throw:{error, closed} ->
            {stop, closed, {error, closed}, State};
        Type:Reason ->
            {ok, [], []} = pgsql:squery(P, "ROLLBACK"),
            {reply, {error, {rollback, Type, Reason}}, State}
    end;
handle_call({dirty, F, Args0}, _From, #state{pid = P} = State) ->
    R = apply(F, [P|Args0]),
    %% XXX: What if exit signal is received here? Response may be lost.
    {reply, {ok, R}, State};
handle_call(close, _From, State) ->
    {stop, shutdown, State};
handle_call(connection, _From, #state{pid = P} = State) ->
    {reply, {ok, P}, State};
handle_call(release, _From, #state{pool = Name} = State) ->
    {reply, do_release(Name), State};
handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast(Msg, _) -> exit({unknown_cast, Msg}).
handle_info(Msg, _) -> exit({unknown_info, Msg}).
code_change(_OldVsn, State, _Extra) -> {ok, State}.

do_release(Name) -> epgsql_connpool:available(Name, self()).
