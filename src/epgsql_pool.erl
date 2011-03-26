%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool).

-behaviour(gen_server).

-export([name/1]).
-export([available/2]).
-export([transaction/2, transaction/3]).

%% gen_server
-export([start_link/1, init/1, code_change/3, terminate/2,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          name,
          size,
          conns = [],
          requests = queue:new(),
          busy = []
         }).

-define(TIMEOUT, timer:seconds(5)).
-define(MAX_WAIT, ?TIMEOUT * 1000).

available(Name, Pid) ->
    gen_server:cast(name(Name), {available, Pid}).

transaction(Name, F) -> transaction(Name, F, []).
transaction(Name, F, Args) ->
    {ok, Pid} = gen_server:call(name(Name), reserve, ?TIMEOUT),
    {ok, CPid} = epgsql_pool_conn:connection(Pid),
    try begin
            {ok, [], []} = pgsql:squery(CPid, "BEGIN"),
            R = apply(F, [CPid|Args]),
            {ok, [], []} = pgsql:squery(CPid, "COMMIT"),
            R
        end
    catch
        throw:Throw ->
            {ok, [], []} = pgsql:squery(CPid, "ROLLBACK"),
            throw(Throw);
        exit:Exit ->
            {ok, [], []} = pgsql:squery(CPid, "ROLLBACK"),
            exit(Exit)
    end.

name(Name) when is_atom(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_pool").

start_link(Name) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, Name, []).

init(Name) ->
    case epgsql_pool_config:pool_size(Name) of
        {ok, Size} ->
            lists:foreach(
              fun(_) -> epgsql_pool_conn_sup:start_connection(Name) end,
              lists:seq(1, Size)),
            {ok, #state{size = Size, name = Name}};
        {error, not_found} ->
            {stop, {error, missing_configuration}}
    end.

terminate(_Reason, #state{}) -> ok.

handle_call(reserve, {Pid, _} = From, #state{conns = [], requests = R} = State) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, State#state{requests = queue:in({From, Ref, erlang:now()}, R)}};
handle_call(reserve, {Pid, _}, #state{conns = [H|T], busy = B} = State) ->
    Ref = erlang:monitor(process, Pid),
    {reply, {ok, H}, State#state{conns = T, busy = [{Ref, H}|B]}};
handle_call(Msg, _, _) -> exit({unknown_call, Msg}).

handle_cast(
  {available, Pid},
  #state{conns = C, requests = R, busy = B0} = State) ->
    F = fun({_, Ts}) ->
                timer:now_diff(erlang:now(), Ts) > ?MAX_WAIT
        end,
    B = case lists:keysearch(Pid, 2, B0) of
            {value, {OldRef, Pid}} ->
                erlang:demonitor(OldRef),
                lists:keydelete(Pid, 2, B0);
            false -> B0
        end,
    case q_dropwhile(F, R) of
        {empty, R} ->
            {noreply, State#state{conns = [Pid|C]}};
        {{value, {Waiter, Ref, _Ts}}, R1} ->
            gen_server:reply(Waiter, {ok, Pid}),
            erlang:yield(),
            {noreply, State#state{requests = R1, busy = [{Ref, Pid}|B]}}
    end;
handle_cast(Msg, _) -> exit({unknown_cast, Msg}).

handle_info(
  {'DOWN', Ref, process, _, _},
  #state{requests = R, busy = B} = State) ->
    case lists:keysearch(Ref, 2, queue:to_list(R)) of
        false ->
            {value, {Ref, Conn}} = lists:keysearch(Ref, 1, B),
            B1 = lists:keydelete(Ref, 1, B),
            true = exit(Conn, rollback),
            {noreply, State#state{busy = B1}};
        {value, {_, Ref, _}} ->
            R1 = queue:from_list(lists:keydelete(Ref, 2, queue:to_list(R))),
            {noreply, State#state{requests = R1}}
    end;
handle_info(Msg, _) -> exit({unknown_info, Msg}).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

q_dropwhile(F, Q) ->
    case queue:out(Q) of
        {empty, Q} -> {empty, Q};
        {{value, I}, Q1} ->
            case F(I) of
                true  -> q_dropwhile(F, Q1);
                false -> {{value, I}, Q1}
            end
    end.
