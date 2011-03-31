%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

name(Name) -> list_to_atom(atom_to_list(Name) ++ "_pool_sup").

start_link(Name) ->
    supervisor:start_link({local, name(Name)}, ?MODULE, [Name]).

init([Name]) ->
    Children
        = [{epgsql_connpool_conn_sup, {epgsql_connpool_conn_sup, start_link, [Name]}, permanent,
            16#ffffffff, supervisor, [epgsql_connpool_conn_sup]},
           {epgsql_connpool, {epgsql_connpool, start_link, [Name]}, permanent,
            16#ffffffff, worker, [epgsql_connpool]}],
    {ok, {{one_for_all, 10, 10}, Children}}.
