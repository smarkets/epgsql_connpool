%% -*- tab-width: 4;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
{application, epgsql_pool,
 [{description, "PostgreSQL Client Pool"},
  {vsn, "1.0.0"},
  {modules, [epgsql_pool, epgsql_pool_app, epgsql_pool_sup]},
  {registered, []},
  {applications, [kernel, stdlib, sasl, public_key, crypto, ssl]},
  {mod, {epgsql_pool_app, []}},
  {env, [{pool_size, 10}]}]}.


