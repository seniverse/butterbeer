-module(butterbeer).

-behaviour(gen_server).

-export([get/2]).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

get(Table, Key) ->
    Policy = persistent_term:get(Table),
    Policy:get(Table, Key).

start_link(Table, Load, Policy) ->
    gen_server:start_link({local, Table}, ?MODULE, [Table, Load, Policy], []).

init([Table, Load, {PolicyMod, PolicyOpts}]) ->
    process_flag(trap_exit, true),
    ok = PolicyMod:start(Table, PolicyOpts),
    persistent_term:put(Table, PolicyMod),

    {ok,
     #{load => Load,
       loaders => #{},
       waitings => #{},
       policy => PolicyMod
      }}.

handle_call({get, Key}, From, State = #{waitings := Waitings, policy := Policy}) ->
    case maps:find(Key, Waitings) of
        error ->
            case Policy:reserve(Key) of
                ok ->
                    State1 = start_load(Key, State),
                    {noreply, State1#{waitings => Waitings#{Key => [From]}}};
                {already_loaded, Data} ->
                    {reply, Data, State}
            end;
        {ok, List} ->
            Policy:wait(Key),
            {noreply, State#{waitings => Waitings#{Key => [From|List]}}}
    end;
handle_call({loaded, Key, Data}, {Pid, _}, State=#{loaders := Loaders, waitings := Waitings, policy := Policy}) ->
    {Key, Loaders1} = maps:take(Pid, Loaders),
    {List, Waitings1} = maps:take(Key, Waitings),
    Policy:loaded(Key, Data),
    [gen_server:reply(W, Data) || W <- List],
    {reply, ok, State#{loaders => Loaders1, waitings => Waitings1}}.

handle_cast({access, Key}, State=#{policy:=Policy}) ->
    Policy:access(Key),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State=#{loaders := Loaders, waitings := Waitings, policy := Policy}) ->
    case maps:take(Pid, Loaders) of
        error ->
            {noreply, State};
        {Key, Loaders1} ->
            {List, Waitings1} = maps:take(Key, Waitings),
            [ From ! {'DOWN', Mref, process, self(), Reason} || {From, Mref} <- List],
            Policy:release(Key),
            {noreply, State#{loaders := Loaders1, waitings := Waitings1}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{policy := Policy}) ->
    Policy:terminate(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

start_load(Key, State=#{loaders := Loaders, load := Load}) ->
    Self = self(),
    Pid = spawn_link(fun () -> do_load(Self, Load, Key) end),
    State#{loaders => Loaders#{Pid => Key}}.

do_load(Parent, {M, F, A}, Key) ->
    ok = gen_server:call(Parent, {loaded, Key, apply(M, F, [Key|A])}).
