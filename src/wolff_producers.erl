%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. 4月 2020 下午2:31
%%%-------------------------------------------------------------------
-module(wolff_producers).
-author("zxb").

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff producers]").

%% API
-export([start_link/3, start_linked_producers/3, stop_linked/1, start_supervised/3, stop_supervised/1, pick_producer/2, lookup_producer/2]).

-export([init/1, handle_info/2, handle_call/3, handle_cast/2, code_change/3, terminate/2]).

-export_type([producers/0]).

-type producers() :: #{workers := #{partition() => pid()} | ets:tab(),
partition_cnt := pos_integer(),
partitioner := partitioner(),
client => wolff:client_id() | pid(),
topic => kpro:topic()
}.

-type topic() :: kpro:topic().

-type partition() :: kpro:partition().

-type config() :: wolff_producer:config().

-type partitioner() :: random | roundrobin | first_key_dispatch
| fun((PartitionCount :: pos_integer(), [wolff:msg()]) -> partition()) | partition().

%% 对外接口
start_link(ClientId, Topic, Config) ->
  ?LOG(warning, "start link... ClientId: ~p, Topic: ~p, Config: ~p", [ClientId, Topic, Config]),
  Name = get_name(Config),
  gen_server:start_link({local, Name}, ?MODULE, {ClientId, Topic, Config}, []).

-spec start_linked_producers(Client, Topic, ProducerCfg) -> {ok, producers()} | {error, any()} when
  Client :: wolff:client_id() | pid(),
  Topic :: topic(),
  ProducerCfg :: config().
start_linked_producers(Client, Topic, ProducerCfg) ->
  ?LOG(warning, "start linked producers... Client: ~p, Topic: ~p, ProducerCfg: ~p", [Client, Topic, ProducerCfg]),
  {ClientId, ClientPid} = case is_binary(Client) of
                            true ->
                              {ok, Pid} = wolff_client_sup:find_client(Client),
                              {Client, Pid};
                            false -> {wolff_client:get_id(Client), Client}
                          end,
  case wolff_client:get_leader_connections(ClientPid, Topic) of
    {ok, Connections} ->
      Workers = start_link_producers(ClientId, Topic, Connections, ProducerCfg),
      Partitioner = maps:get(partitioner, ProducerCfg, random),
      {ok, #{client => Client,
        topic => Topic,
        workers => Workers,
        partition_cnt => maps:size(Workers),
        partitioner => Partitioner}};
    {error, Reason} -> {error, Reason}
  end.

stop_linked(#{workers := Workers}) when is_map(Workers) ->
  ?LOG(warning, "stop linked... Workers: ~p", [Workers]),
  lists:foreach(fun({_, Pid}) -> wolff_producer:stop(Pid) end, maps:to_list(Workers)).

-spec start_supervised(wolff:client_id(), topic(), config()) -> {ok, producers()}.
start_supervised(ClientId, Topic, ProducerCfg) ->
  ?LOG(warning, "start supervised... ClientId: ~p, Topic: ~p, ProducerCfg: ~p", [ClientId, Topic, ProducerCfg]),
  {ok, Pid} = wolff_producers_sup:ensure_present(ClientId, Topic, ProducerCfg),
  case gen_server:call(Pid, get_workers, infinity) of
    {0, not_initialized} ->
      {error, failed_to_initialize_producers_in_time};
    {Cnt, Ets} ->
      {ok, #{client => ClientId, topic => Topic, workers => Ets,
        partition_cnt => Cnt, partitioner => maps:get(partitioner, ProducerCfg, random)}}
  end.

stop_supervised(#{client := ClientId, workers := Workers}) ->
  ?LOG(warning, "stop supervised... ClientId: ~p, Workers: ~p", [ClientId, Workers]),
  wolff_producers_sup:ensure_absence(ClientId, Workers).

-spec pick_producer(producers(), [wolff:msg()]) -> {partition(), pid()}.
pick_producer(#{workers := Workers, partition_cnt := Count, partitioner := Partitioner}, Batch) ->
  ?LOG(warning, "pick producer... Workers: ~p, Count: ~p, Partitioner: ~p, Batch: ~p", [Workers, Count, Partitioner, Batch]),
  Partition = pick_partition(Count, Partitioner, Batch),
  do_pick_producer(Partitioner, Partition, Count, Workers).

lookup_producer(#{workers := Workers}, Partition) ->
  lookup_producer(Workers, Partition);
lookup_producer(Workers, Partition) when is_map(Workers) ->
  maps:get(Partition, Workers);
lookup_producer(Workers, Partition) ->
  [{Partition, Pid}] = ets:lookup(Workers, Partition),
  Pid.

%% 回调接口
init({ClientId, Topic, Config}) ->
  ?LOG(warning, "init... ClientId: ~p, Topic: ~p, Config: ~p", [ClientId, Topic, Config]),
  erlang:process_flag(trace_exit, true),
  self() ! rediscover_client,
  {ok, #{client_id => ClientId,
    client_pid => false,
    topic => Topic,
    config => Config,
    ets => not_initialized,
    partition_cnt => 0}}.

handle_info(rediscover_client, #{client_id := ClientId, client_pid := false} = State) ->
  ?LOG(warning, "handle info rediscover_client... State: ~p", [State]),
  State1 = State#{rediscover_client_tref => false},
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      _ = erlang:monitor(process, Pid),
      State2 = State1#{client_pid := Pid},
      State3 = maybe_init_producers(State2),
      NewState = maybe_restart_producers(State3),
      {noreply, NewState};
    {error, Reason} ->
      ?LOG(warning, "Failed to discover client, reason = ~p", [Reason])
  end;
handle_info(init_producers, State) ->
  ?LOG(warning, "handle info init_producers... State: ~p", [State]),
  {noreply, maybe_init_producers(State)};
handle_info({'DOWN', _, process, Pid, Reason}, #{client_id := ClientId, client_pid := Pid} = State) ->
  ?LOG(warning, "handle info DOWN... Client ~p (pid = ~p) down, reason: ~p", [ClientId, Pid, Reason]),
  {noreply, ensure_rediscover_client_timer(State#{client_pid := false})};
handle_info({'EXIT', Pid, Reason},
    #{ets := Ets, topic := Topic, client_id := ClientId, client_pid := ClientPid, config := Config} = State) ->
  ?LOG(warning, "handle info EXIT... State: ~p", [State]),
  case ets:match(Ets, {'$1', Pid}) of
    [] ->
      ?LOG(warning, "Unknown EXIT message of pid ~p reason: ~p", [Pid, Reason]);
    [[Partition]] ->
      case is_alive(ClientPid) of
        true ->
          ?LOG(warning, "Producer ~s-~p (pid = ~p) down\nreason: ~p", [Topic, Partition, Pid, Reason]),
          ok = start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config);
        false ->
          ets:insert(Ets, {Partition, {down, Reason}})
      end
  end,
  {noreply, State};
handle_info(Req, State) ->
  ?LOG(warning, "handle info... Unknown info ~p", [Req]),
  {noreply, State}.

handle_call(get_workers, _From, #{ets := Ets, partition_cnt := Cnt} = State) ->
  ?LOG(warning, "handle call get_workers... State: ~p", State),
  {reply, {Cnt, Ets}, State};
handle_call(Req, From, State) ->
  ?LOG(warning, "handle call... Unknown call ~p from ~p", [Req, From]),
  {reply, {error, unknown_call}, State}.

handle_cast(Req, State) ->
  ?LOG(warning, "handle cast... Unknown cast ~p", [Req]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  ?LOG(warning, "code change..."),
  {ok, State}.

terminate(_, _State) ->
  ?LOG(warning, "terminate... State: ~p", [_State]),
  ok.

%% 内部接口
get_name(Config) ->
  maps:get(name, Config, wolff_producers).

start_link_producers(ClientId, Topic, Connections, ProducerCfg) ->
  lists:foldl(
    fun({Partition, MaybeConnPid}, Acc) ->
      {ok, WorkerPid} = wolff_producer:start_link(ClientId, Topic, Partition, MaybeConnPid, ProducerCfg),
      Acc#{Partition => WorkerPid}
    end, #{}, Connections).

pick_partition(_Count, Partition, _) when is_integer(Partition) ->
  Partition;
pick_partition(Count, random, _) ->
  rand:uniform(Count) - 1;
pick_partition(Count, roundrobin, _) ->
  Partition = case get(wolff_roundrobin) of
                undefined -> 0;
                Number -> Number
              end,
  _ = put(wolff_roundrobin, (Partition + 1) rem Count),
  Partition;
pick_partition(Count, first_key_dispatch, [#{key := Key} | _]) ->
  erlang:phash2(Key) rem Count;
pick_partition(Count, F, Batch) -> F(Count, Batch).

do_pick_producer(Partitioner, Partition, Count, Workers) ->
  Pid = lookup_producer(Workers, Partition),
  case is_pid(Pid) andalso is_process_alive(Pid) of
    true -> {Partition, Pid};
    false when Partitioner =:= random ->
      pick_next_alive(Workers, Partition, Count);
    false when Partitioner =:= roundrobin ->
      R = {Partition, Pid} = pick_next_alive(Workers, Partition, Count),
      _ = put(wolff_roundrobin, (Partition + 1) rem Count),
      R;
    false ->
      erlang:error({producer_down, Pid})
  end.

pick_next_alive(Workers, Partition, Count) ->
  pick_next_alive(Workers, (Partition + 1) rem Count, Count, _Tried = 1).

pick_next_alive(_Workers, _Partition, Count, Count) ->
  erlang:error(all_producers_down);
pick_next_alive(Workers, Partition, Count, Tried) ->
  Pid = lookup_producer(Workers, Partition),
  case is_alive(Pid) of
    true -> {Partition, Pid};
    false -> pick_next_alive(Workers, (Partition + 1) rem Count, Count, Tried + 1)
  end.

is_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

maybe_init_producers(#{ets := not_initialized, topic := Topic, client_id := ClientId, config := Config} = State) ->
  case start_linked_producers(ClientId, Topic, Config) of
    {ok, #{workers := Workers}} ->
      Ets = ets:new(get_name(Config), [protected, named_table]),
      true = ets:insert(Ets, maps:to_list(Workers)),
      State#{ets := Ets, partition_cnt => maps:size(Workers)};
    {error, Reason} ->
      ?LOG(warning, "Failed to init producers for topic ~s, reason: ~p", [Topic, Reason]),
      erlang:send_after(1000, self(), init_producers),
      State
  end;
maybe_init_producers(State) -> State.

maybe_restart_producers(#{ets := not_initialized} = State) -> State;
maybe_restart_producers(#{ets := Ets, client_id := ClientId, topic := Topic, config := Config} = State) ->
  lists:foreach(
    fun({Partition, Pid}) ->
      case is_alive(Pid) of
        true -> ok;
        false ->
          start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config)
      end
    end, ets:tab2list(Ets)),
  State.

ensure_rediscover_client_timer(#{rediscover_client_tref := false} = State) ->
  TimerRef = erlang:send_after(1000, self(), rediscover_client),
  State#{rediscover_client_tref := TimerRef}.

start_producer_and_insert_pid(Ets, ClientId, Topic, Partition, Config) ->
  {ok, Pid} = wolff_producer:start_link(ClientId, Topic, Partition, {down, to_be_discovered}, Config),
  ets:insert(Ets, {Partition, Pid}),
  ok.