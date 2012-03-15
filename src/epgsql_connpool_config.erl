%% Copyright (c) 2011 Smarkets Limited
%% Distributed under the MIT license; see LICENSE for details.
-module(epgsql_connpool_config).

-export([pools/0, pool_size/1, by_name/1, conn_by_name/1]).
-export([add_pool/2]).

-define(DEFAULT_SZ, 10).

-type pool_name() :: atom().

-spec add_pool(pool_name(), [tuple()]) -> ok.
add_pool(Name, Config) ->
    L0 = pools(),
    L = [{Name, Config}|L0],
    application:set_env(epgsql_connpool, pools, L).

-spec pools() -> [{pool_name(), list()}].
pools() ->
    case application:get_env(epgsql_connpool, pools) of
        undefined -> [];
        {ok, L}   -> L
    end.

-spec pool_size(pool_name()) -> {error, not_found} | {ok, pos_integer()}.
pool_size(Name) ->
    case by_name(Name) of
        {error, not_found} -> {error, not_found};
        {ok, Config} ->
            case lookup(size, Config) of
                {ok, V} when is_integer(V), V > 0 -> {ok, V};
                _ -> {ok, ?DEFAULT_SZ}
            end
    end.

-spec by_name(pool_name()) -> {error, not_found} | {ok, list()}.
by_name(Name) -> lookup(Name, pools()).

-spec conn_by_name(pool_name()) -> {error, not_found} | {ok, list()}.
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

-type config_val() :: [tuple()].
-type lookup_result() :: {error, not_found} | {ok, term()}.
-spec lookup(atom(), config_val()) -> lookup_result().
lookup(K, L) when is_list(L) ->
    case lks(K, L) of
        false           -> {error, not_found};
        {value, {K, V}} -> {ok, V}
    end.

-spec lks(term(), [tuple()]) -> {value, tuple()} | false.
%% lks(K, L) -> lists:keysearch(K, 1, L).
lks(K, L) ->
    case proplists:get_value(K, L) of
        undefined -> false;
        V -> {value, {K, V}}
    end.

