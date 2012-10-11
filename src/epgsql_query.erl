%% Copyright (c) 2012 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_query).

-export([squery/1,
         equery/1,
         equery/2]).
-export([parse/1,
         parse/2,
         parse/3]).
-export([describe/1,
         describe/2]).
-export([bind/2,
         bind/3]).
-export([execute/1,
         execute/2,
         execute/3]).
-export([close/1, sync/0]).
-export([get_parameter/1]).

squery(Sql) ->
    pgsql:squery(cpid(), Sql).

equery(Sql) ->
    pgsql:equery(cpid(), Sql).

equery(Sql, Parameters) ->
    pgsql:equery(cpid(), Sql, Parameters).

parse(Sql) ->
    pgsql:parse(cpid(), Sql).

parse(Sql, Types) ->
    pgsql:parse(cpid(), Sql, Types).

parse(Name, Sql, Types) ->
    pgsql:parse(cpid(), Name, Sql, Types).

bind(Statement, Parameters) ->
    pgsql:bind(cpid(), Statement, Parameters).

bind(Statement, PortalName, Parameters) ->
    pgsql:bind(cpid(), Statement, PortalName, Parameters).

execute(S) ->
    pgsql:execute(cpid(), S).

execute(S, N) ->
    pgsql:execute(cpid(), S, N).

execute(S, PortalName, N) ->
    pgsql:execute(cpid(), S, PortalName, N).

describe(Statement) ->
    pgsql:describe(cpid(), Statement).

describe(Type, Name) ->
    pgsql:describe(cpid(), Type, Name).

close(S) ->
    pgsql:close(cpid(), S).

sync() ->
    pgsql:sync(cpid()).

get_parameter(Name) ->
    pgsql:get_parameter(cpid(), Name).

cpid() ->
    case get(epgsql_conn) of
        Pid when is_pid(Pid) -> Pid;
        _ -> exit(no_connection)
    end.
