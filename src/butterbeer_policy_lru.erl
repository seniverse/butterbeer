-module(butterbeer_policy_lru).

-export([get/2]).

-export([start/2, reserve/1, loaded/2, wait/1, access/1, release/1, terminate/0]).

get(Table, Key) ->
    case ets:lookup(Table, Key) of
        [] ->
            gen_server:call(Table, {get, Key});
        [{Key, Data}] ->
            gen_server:cast(Table, {access, Key}),
            Data
    end.

start(Table, Opts) ->
    Table =
        ets:new(
          Table,
          [set,
           protected,
           named_table,
           {read_concurrency,true}]),

    Size = maps:get(size, Opts, 1024),

    put(?MODULE,
        #{table => Table,
          size => Size,
          count => 0,
          queue => heapq:new()
         }),
    ok.

reserve(Key) ->
    #{table := Table,
      size := Size,
      count := Count,
      queue := Queue} = State = get(?MODULE),
    case ets:lookup(Table, Key) of
        [{Key, Data}] ->
            {already_loaded, Data};
        [] ->
            case heapq:size(Queue) of
                Size ->
                    {K, _, Queue1} = heapq:delete_min(Queue),
                    true = ets:delete(Table, K);
                Size1 when Size1 < Size ->
                    Queue1 = Queue
            end,
            put(?MODULE, State#{count := Count + 1, queue := heapq:insert(Key, Count, Queue1)}),
            ok
    end.

loaded(Key, Value) ->
    #{table := Table} = get(?MODULE),
    ets:insert(Table, {Key, Value}).

wait(Key) ->
    access(Key).

access(Key) ->
    #{count := Count, queue := Queue} = State = get(?MODULE),
    put(?MODULE, State#{count := Count + 1, queue := heapq:update_key(Key, fun(_) -> Count end, Queue)}).

release(Key) ->
    #{queue := Queue} = State = get(?MODULE),
    put(?MODULE, State#{queue := heapq:delete_key(Key, Queue)}).

terminate() ->
    ok.
