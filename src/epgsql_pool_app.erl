%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_pool_app).

-behaviour(application).

-export([start/2, stop/1]).

start(normal, _Args) ->
    case epgsql_pool_sup:start_link() of
        {ok, Pid}       -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

stop(_State) -> ok.
