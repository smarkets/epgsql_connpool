%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool).

-behaviour(gen_server).

-export([name/1]).
-export([available/2, status/1, reserve/2, release/2]).
-export([transaction/2, transaction/3, transaction/4]).

%% gen_server
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          name,
          min_size,
          max_size = 0,
          busy     = 0, % count of busy connections
          requests = queue:new(),
          conns    = queue:new(),
          tab      = gb_trees:empty()
         }).

-record(req, {
          from,
          pid,
          ref,
          timestamp,
          timeout
         }).

-define(TIMEOUT, timer:seconds(5)).

status(Name) -> gen_server:call(name(Name), stats, infinity).

available(Name, Pid) ->
    gen_server:cast(name(Name), {available, Pid}).

reserve(Name, Timeout) ->
    gen_server:call(name(Name), {reserve, self(), Timeout}, infinity).

release(Name, Pid) ->
    gen_server:cast(name(Name), {release, self(), Pid}).

transaction(Name, F) -> transaction(Name, F, ?TIMEOUT).
transaction(Name, F, Timeout) -> transaction(Name, F, [], Timeout).
transaction(Name, F, Args, Timeout) ->
    case reserve(Name, Timeout) of
        {ok, Pid} when is_pid(Pid) ->
            % If we exit here, the transaction never gets started
            try begin
                    % Whereas an exit here will end up calling the 'after' block
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
                ok = release(Name, Pid)
            end;
        {error, reserve_timeout} -> {aborted, {error, reserve_timeout}}
    end.

name(Name) when is_atom(Name) -> list_to_atom(atom_to_list(Name) ++ "_pool").

start_link(Name) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, Name, []).

init(Name) ->
    case epgsql_pool_config:pool_size(Name) of
        {ok, Size} ->
            S = #state{min_size = Size, name = Name},
            ok = ensure_min(S),
            {ok, S};
        {error, not_found} -> {stop, {error, missing_configuration}}
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

handle_call({reserve, Pid, Timeout}, From, #state{conns = C} = State0) ->
    Ref = erlang:monitor(process, Pid),
    State = queue_request(Pid, Ref, From, Timeout, State0),
    case queue:is_empty(C) of
        % No connections available
        true  -> {noreply, State};
        % Immediately hand off connection in reply
        false -> {noreply, dequeue_request(State)}
    end;

handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast({release, RPid, PPid}, State) ->
    {noreply, connection_returned(RPid, PPid, State)};
handle_cast({available, CPid}, State) ->
    {noreply, connection_available(CPid, State)};
handle_cast(Msg, _) -> exit({unknown_cast, Msg}).

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    {noreply, process_died(Pid, Ref, State)};
handle_info(Msg, _) -> exit({unknown_info, Msg}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

ensure_min(#state{min_size = Sz, name = Name, conns = C, busy = B}) ->
    Need = max(0, Sz - queue:len(C) - B),
    ok = lists:foreach(
           fun(_) ->
                   epgsql_pool_conn_sup:start_connection(Name)
           end, lists:seq(1, Need)).

connection_returned(RPid, PPid, #state{tab = T0, busy = B0} = S) ->
    {CPid, T1} = tree_pop(PPid, T0),
    {{RRef, CPid}, T2} = tree_pop(RPid, T1),
    {{RPid, busy_request}, T3} = tree_pop(RRef, T2),
    {{CRef, RPid}, T4} = tree_pop(CPid, T3),
    {{CPid, busy_connection}, T5} = tree_pop(CRef, T4),
    true = erlang:demonitor(CRef, [flush]),
    true = erlang:demonitor(RRef, [flush]),
    ok = epgsql_pool_conn:release(CPid),
    S#state{tab = T5, busy = B0 - 1}.

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
                            {ok, PPid} = epgsql_pool_conn:connection(CPid),
                            T4 = gb_trees:insert(PPid, CPid, T3),
                            T5 = gb_trees:insert(CRef, {CPid, busy_connection}, T4),
                            T6 = gb_trees:insert(CPid, {CRef, RPid}, T5),
                            T7 = gb_trees:insert(RPid, {RRef, CPid}, T6),
                            T8 = gb_trees:insert(RRef, {RPid, busy_request}, T7),
                            gen_server:reply(RFrom, {ok, PPid}),
                            dequeue_request(S1#state{tab = T8, conns = C, busy = S1#state.busy + 1})
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
            {ok, PPid} = epgsql_pool_conn:connection(CPid),
            ok = epgsql_pool_conn:release(CPid),
            {CPid, T5} = tree_pop(PPid, T4),
            C = queue:in(CPid, C0),
            S#state{tab = T5, conns = C, busy = B0 - 1};
        {{RPid, waiting_request}, T1} ->
            RPid = Pid,
            {RRef, T2} = tree_pop(RPid, T1),
            RRef = Ref,
            R = q_delete(RRef, #req.ref, R0),
            S#state{tab = T2, requests = R}
    end.

tree_pop(K, T) -> {gb_trees:get(K, T), gb_trees:delete(K, T)}.

q_delete(Item, Q) -> queue:from_list(lists:delete(Item, queue:to_list(Q))).
q_delete(Key, I, Q) -> queue:from_list(lists:keydelete(Key, I, queue:to_list(Q))).
