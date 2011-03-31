%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool_conn_sup).

-behaviour(supervisor).

-export([name/1]).
-export([start_link/1, start_connection/1]).

-export([init/1]).

name(Name) -> list_to_atom(atom_to_list(Name) ++ "_pool_conn_sup").

start_link(Name) -> supervisor:start_link({local, name(Name)}, ?MODULE, [Name]).

start_connection(Name) -> supervisor:start_child(name(Name), []).

init([Name]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{undefined,
            {epgsql_connpool_conn, start_link, [Name]},
            transient, brutal_kill, worker, [epgsql_connpool_conn]}]}}.
