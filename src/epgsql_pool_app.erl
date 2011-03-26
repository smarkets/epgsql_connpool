%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool_app).

-behaviour(supervisor).
-behaviour(application).

%% supervisor
-export([start_link/0, init/1]).

%% application
-export([start/2, stop/1]).

%% supervisor
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Names = [X || {X, _} <- epgsql_pool_config:pools()],
    Children
        = [{Name, {epgsql_pool_sup, start_link, [Name]}, permanent,
            16#ffffffff, supervisor, [epgsql_pool_sup]} || Name <- Names],
    {ok, {{one_for_one, 10, 10}, Children}}.

%% application
start(normal, _Args) ->
    case ?MODULE:start_link() of
        {ok, Pid}       -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

stop(_State) -> ok.
