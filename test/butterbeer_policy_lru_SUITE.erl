-module(butterbeer_policy_lru_SUITE).

-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [test_cache_hit, test_cache_miss, test_load_crash].

load(Key, Ref) ->
    ok = atomics:add(Ref, 1, 1),
    Key.

crash(_Key) ->
    exit(crash).

end_per_testcase(_Case, _Config) ->
    gen_server:stop(?MODULE).

test_cache_hit(_Config) ->
    Ref = atomics:new(1, []),
    butterbeer:start_link(
      ?MODULE,
      {?MODULE, load, [Ref]},
      {butterbeer_policy_lru, #{size => 1}}),
    ok = atomics:put(Ref, 1, 0),
    <<"a">> = butterbeer:get(?MODULE, <<"a">>),
    1 = atomics:get(Ref, 1),
    <<"a">> = butterbeer:get(?MODULE, <<"a">>),
    1 = atomics:get(Ref, 1),
    ok.

test_cache_miss(_Config) ->
    Ref = atomics:new(1, []),
    butterbeer:start_link(
      ?MODULE,
      {?MODULE, load, [Ref]},
      {butterbeer_policy_lru, #{size => 1}}),
    ok = atomics:put(Ref, 1, 0),
    <<"a">> = butterbeer:get(?MODULE, <<"a">>),
    1 = atomics:get(Ref, 1),
    <<"b">> = butterbeer:get(?MODULE, <<"b">>),
    2 = atomics:get(Ref, 1),
    ok.

test_load_crash(_Config) ->
    butterbeer:start_link(
      ?MODULE,
      {?MODULE, crash, []},
      {butterbeer_policy_lru, #{size => 1}}),
    {'EXIT', {crash, _}} = (catch butterbeer:get(?MODULE, <<"a">>)),
    ok.
