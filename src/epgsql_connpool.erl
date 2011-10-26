%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool).

-behaviour(gen_server).

-export([name/1]).
-export([available/2, status/1, reserve/2, release/2]).
-export([transaction/2, transaction/3, transaction/4]).
-export([dirty/2, dirty/3, dirty/4]).

%% gen_server
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          name,
          min_size,
          max_size  = 0,
          incr_size = 0,
          busy      = 0, % count of busy connections
          requests  = queue:new(),
          conns     = queue:new(),
          tab       = gb_trees:empty()
         }).

-record(req, {
          from,
          pid,
          ref,
          timestamp,
          timeout
         }).

-define(DEFAULT_RES_TIMEOUT, timer:seconds(5)).
-define(DEFAULT_TXN_TIMEOUT, infinity).

status(Name) -> gen_server:call(name(Name), stats, infinity).

available(Name, Pid) ->
    gen_server:cast(name(Name), {available, Pid}).

reserve(Name, Timeout) ->
    gen_server:call(name(Name), {reserve, self(), Timeout}, infinity).

release(Name, Pid) ->
    gen_server:cast(name(Name), {release, self(), Pid}).


transaction(Name, F) -> transaction(Name, F, []).
transaction(Name, F, Opts) -> transaction(Name, F, [], Opts).
transaction(Name, F, Args, Opts) ->
    RTimeout = proplists:get_value(reserve_timeout, Opts, ?DEFAULT_RES_TIMEOUT),
    TTimeout = proplists:get_value(transaction_timeout, Opts, ?DEFAULT_TXN_TIMEOUT),
    case reserve(Name, RTimeout) of
        {ok, Pid} when is_pid(Pid) -> transaction(Name, Pid, F, Args, Opts, TTimeout);
        {error, reserve_timeout}   -> {aborted, {error, reserve_timeout}}
    end.

transaction(Name, CPid, F, Args, _Opts, infinity) ->
    {ok, Pid} = epgsql_connpool_conn:connection(CPid),
    try begin
            {ok, [], []} = pgsql:squery(Pid, "BEGIN"),
            R = apply(F, [Pid|Args]),
            {ok, [], []} = pgsql:squery(Pid, "COMMIT"),
            {atomic, R}
        end
    catch
        throw:Throw ->
            {ok, [], []} = pgsql:squery(Pid, "ROLLBACK"),
            throw(Throw);
        exit:Exit ->
            {ok, [], []} = pgsql:squery(Pid, "ROLLBACK"),
            exit(Exit)
    after
        ok = release(Name, CPid)
    end;
transaction(Name, CPid, F, Args, _Opts, TTimeout) ->
    case epgsql_connpool_conn:transaction(CPid, F, Args, TTimeout) of
        {ok, R} ->
            ok = release(Name, CPid),
            {atomic, R};
        {error, transaction_timeout} = E ->
            {aborted, E}
    end.


dirty(Name, F) -> dirty(Name, F, []).
dirty(Name, F, Opts) -> dirty(Name, F, [], Opts).
dirty(Name, F, Args, Opts) ->
    RTimeout = proplists:get_value(reserve_timeout, Opts, ?DEFAULT_RES_TIMEOUT),
    TTimeout = proplists:get_value(transaction_timeout, Opts, ?DEFAULT_TXN_TIMEOUT),
    case reserve(Name, RTimeout) of
        {ok, Pid} when is_pid(Pid) -> dirty(Name, Pid, F, Args, Opts, TTimeout);
        {error, reserve_timeout}   -> {aborted, {error, reserve_timeout}}
    end.

dirty(Name, CPid, F, Args, _Opts, infinity) ->
    {ok, Pid} = epgsql_connpool_conn:connection(CPid),
    try begin
            R = apply(F, [Pid|Args]),
            {dirty, R}
        end
    catch
        throw:Throw ->
            throw(Throw);
        exit:Exit ->
            exit(Exit)
    after
        ok = release(Name, CPid)
    end;
dirty(Name, CPid, F, Args, _Opts, TTimeout) ->
    case epgsql_connpool_conn:dirty(CPid, F, Args, TTimeout) of
        {ok, R} ->
            ok = release(Name, CPid),
            {dirty, R};
        {error, transaction_timeout} = E ->
            {aborted, E}
    end.

name(Name) when is_atom(Name) -> list_to_atom(atom_to_list(Name) ++ "_pool").

start_link(Name) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, Name, []).

init(Name) ->
    PoolSize = epgsql_connpool_config:pool_size(Name),
    MinSize = epgsql_connpool_config:min_size(Name),
    IncrSize = epgsql_connpool_config:incr_size(Name),
    case {PoolSize, MinSize, IncrSize} of
        {{ok, PS}, {ok, MS}, {ok, IS}} ->
            S = #state{max_size = PS, min_size = MS, incr_size = IS, name = Name},
            ok = ensure_min(S),
            {ok, S};
        {_, _, _} -> {stop, {error, missing_configuration}}
    end.

terminate(_Reason, #state{}) -> ok.

handle_call(
  stats,
  _From,
  #state{min_size = MinSz,
         max_size = MaxSz,
         busy = B,
         tab = T,
         requests = R,
         conns = C} = State) ->
    M = length(
          lists:filter(
            fun({_, {Pid, _}}) when is_pid(Pid) -> true; (_) -> false end,
            gb_trees:to_list(T))),
    {reply,
     {ok, [{min_size, MinSz},
           {max_size, MaxSz},
           {available, queue:len(C)},
           {requests, queue:len(R)},
           {busy, B},
           {monitored, M}]},
     State};

handle_call({reserve, Pid, Timeout}, From, #state{conns = C, busy = B, max_size = MaxSize, incr_size=IncrSize, name = Name} = State0) ->
    Ref = erlang:monitor(process, Pid),
    State = queue_request(Pid, Ref, From, Timeout, State0),
    case queue:is_empty(C) of
        % No connections available
        true when B >= MaxSize -> {noreply, State};
        % Grow pool by increment size
        true -> lists:foreach(fun(_) -> gen_server:cast(name(Name), start_connection) end, 
                              lists:seq(1, min(IncrSize, MaxSize - B))),
                {noreply, State};
        % Immediately hand off connection in reply
        false -> {noreply, dequeue_request(State)}
    end;

handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast(start_connection, State=#state{name=Name}) ->
    start_connection(Name), 
    {noreply, State};
handle_cast({release, RPid, CPid}, State) ->
    {noreply, connection_returned(RPid, CPid, State)};
handle_cast({available, CPid}, State) ->
    {noreply, connection_available(CPid, State)};
handle_cast({free, CPid}, State) ->
    ok = epgsql_connpool_conn:close(CPid),
    {noreply, State};
handle_cast(Msg, _) -> exit({unknown_cast, Msg}).

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    {noreply, process_died(Pid, Ref, State)};
handle_info(Msg, _) -> exit({unknown_info, Msg}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

ensure_min(#state{min_size = Sz, name = Name, conns = C, busy = B}) ->
    Need = max(0, Sz - queue:len(C) - B),
    ok = lists:foreach(fun(_) -> gen_server:cast(name(Name), start_connection) end, lists:seq(1, Need)).

connection_returned(RPid, CPid, #state{tab = T0, busy = B0, requests = R0, conns = C0, min_size = MinSize, name = Name} = S) ->
    {{RRef, CPid}, T2} = tree_pop(RPid, T0),
    {{RPid, busy_request}, T3} = tree_pop(RRef, T2),
    {{CRef, RPid}, T4} = tree_pop(CPid, T3),
    {{CPid, busy_connection}, T5} = tree_pop(CRef, T4),
    true = erlang:demonitor(CRef, [flush]),
    true = erlang:demonitor(RRef, [flush]),
    case {queue:len(R0), queue:len(C0)} of 
      %% close connection if more than minimum amount of connections available
      %% and no pending requests 
      {0, NumConns} when NumConns >= MinSize -> 
           gen_server:cast(name(Name), {free, CPid}),
           S#state{tab = T5, busy = B0 - 1, conns = q_delete(CPid, C0)};
      {_,_} -> ok = epgsql_connpool_conn:release(CPid), 
           S#state{tab = T5, busy = B0 - 1}
    end.

connection_available(CPid, #state{tab = T0, conns = C0} = S) ->
    CRef = erlang:monitor(process, CPid),
    C = queue:in(CPid, C0),
    T = gb_trees:insert(CPid, CRef, T0),
    T1 = gb_trees:insert(CRef, {CPid, available_connection}, T),
    dequeue_request(S#state{tab = T1, conns = C}).

queue_request(RPid, RRef, RFrom, Timeout, #state{tab = T0, requests = R0} = S) ->
    Item = #req{from = RFrom, pid = RPid, ref = RRef, timestamp = erlang:now(), timeout = Timeout},
    R = queue:in(Item, R0),
    T = gb_trees:insert(RRef, {RPid, waiting_request}, T0),
    T1 = gb_trees:insert(RPid, RRef, T),
    S#state{requests = R, tab = T1}.

dequeue_request(#state{tab = T0, requests = R0, conns = C0} = S) ->
    case queue:out(R0) of
        {empty, R0} -> S;
        {{value, Item}, R} ->
            #req{from = RFrom, pid = RPid, ref = RRef, timestamp = Ts, timeout = Timeout} = Item,
            {{RPid, waiting_request}, T} = tree_pop(RRef, T0),
            {RRef, T1} = tree_pop(RPid, T),
            S1 = S#state{tab = T1, requests = R},
            case timer:now_diff(erlang:now(), Ts) > (Timeout * 1000) of
                % Discard request which timed out
                true ->
                    gen_server:reply(RFrom, {error, reserve_timeout}),
                    true = erlang:demonitor(RRef, [flush]),
                    dequeue_request(S1);
                % Otherwise, reply and track new busy pair
                false ->
                    case queue:out(C0) of
                        % Revert to original queues as no connections are available
                        {empty, C0} -> S;
                        {{value, CPid}, C} ->
                            {CRef, T2} = tree_pop(CPid, S1#state.tab),
                            {{CPid, available_connection}, T3} = tree_pop(CRef, T2),
                            T4 = gb_trees:insert(CRef, {CPid, busy_connection}, T3),
                            T5 = gb_trees:insert(CPid, {CRef, RPid}, T4),
                            T6 = gb_trees:insert(RPid, {RRef, CPid}, T5),
                            T7 = gb_trees:insert(RRef, {RPid, busy_request}, T6),
                            gen_server:reply(RFrom, {ok, CPid}),
                            dequeue_request(S1#state{tab = T7, conns = C, busy = S1#state.busy + 1})
                    end
            end
    end.

process_died(Pid, Ref, #state{tab = T0, conns = C0, requests = R0, busy = B0} = S) ->
    case tree_pop(Ref, T0) of
        {{CPid, busy_connection}, T1} ->
            CPid = Pid,
            {{CRef, RPid}, T2} = tree_pop(CPid, T1),
            CRef = Ref,
            {{RRef, CPid}, T3} = tree_pop(RPid, T2),
            {{RPid, busy_request}, T4} = tree_pop(RRef, T3),
            true = erlang:demonitor(RRef, [flush]),
            S#state{tab = T4, busy = B0 - 1};
        {{CPid, available_connection}, T1} ->
            CPid = Pid,
            {CRef, T2} = tree_pop(CPid, T1),
            CRef = Ref,
            C = q_delete(CPid, C0),
            S#state{tab = T2, conns = C};
        {{RPid, busy_request}, T1} ->
            RPid = Pid,
            {{RRef, CPid}, T2} = tree_pop(RPid, T1),
            RRef = Ref,
            {{CRef, RPid}, T3} = tree_pop(CPid, T2),
            {{CPid, busy_connection}, T4} = tree_pop(CRef, T3),
            ok = epgsql_connpool_conn:release(CPid),
            C = queue:in(CPid, C0),
            true = erlang:demonitor(CRef, [flush]),
            S#state{tab = T4, conns = C, busy = B0 - 1};
        {{RPid, waiting_request}, T1} ->
            RPid = Pid,
            {RRef, T2} = tree_pop(RPid, T1),
            RRef = Ref,
            R = q_delete(RRef, #req.ref, R0),
            S#state{tab = T2, requests = R}
    end.

start_connection(Name) ->
    % FIXME : handle error properly
    epgsql_connpool_conn_sup:start_connection(Name),
    %% {ok, Pid} = epgsql_connpool_conn_sup:start_connection(Name),
    %% true = is_pid(Pid),
    ok.

tree_pop(K, T) -> {gb_trees:get(K, T), gb_trees:delete(K, T)}.

q_delete(Item, Q) -> queue:from_list(lists:delete(Item, queue:to_list(Q))).
q_delete(Key, I, Q) -> queue:from_list(lists:keydelete(Key, I, queue:to_list(Q))).
