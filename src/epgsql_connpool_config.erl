%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool_config).

-export([pools/0, pool_size/1, by_name/1, conn_by_name/1]).

-define(DEFAULT_SZ, 10).

pools() ->
    case application:get_env(epgsql_connpool, pools) of
        undefined -> undefined;
        {ok, L}   -> L
    end.

-spec pool_size(atom()) -> {error, not_found} | {ok, pos_integer()}.
pool_size(Name) ->
    case by_name(Name) of
        {error, not_found} -> {error, not_found};
        {ok, Config} ->
            case lookup(size, Config) of
                {error, not_found} -> {ok, ?DEFAULT_SZ};
                {ok, V} -> {ok, V}
            end
    end.

by_name(Name) -> lookup(Name, pools()).
conn_by_name(Name) ->
    case by_name(Name) of
        {error, not_found} ->
            {error, not_found};
        {ok, Config} ->
            Host = lookup(host, Config),
            Username = lookup(username, Config),
            Password = lookup(password, Config),
            Opts = lookup(opts, Config),
            L = [Host, Username, Password, Opts],
            case lists:member({error, not_found}, L) of
                true  -> {error, not_found};
                false -> {ok, [X || {ok, X} <- L]}
            end
    end.

% Simplification for proplists
lookup(_, undefined) -> {error, not_found};
lookup(K, L) ->
    case lists:keysearch(K, 1, L) of
        false           -> {error, not_found};
        {value, {K, V}} -> {ok, V}
    end.
