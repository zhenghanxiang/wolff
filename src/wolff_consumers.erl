%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. 5月 2020 上午10:16
%%%-------------------------------------------------------------------
-module(wolff_consumers).
-author("zxb").

-behaviour(gen_server).

-include("wolff.hrl").

-logger_header("[wolff consumers]").

%% API
-export([
  start_link/3,
  find_consumer/3
]).

-export([
  init/1,
  handle_info/2,
  handle_call/3,
  code_change/3,
  terminate/2
]).

-type topic() :: kpro:topic().
-type config() :: wolff:config().
-type partition() :: wolff:partition().

-record(state, {
  client_id :: wolff:client_id(),
  client_pid :: pid(),
  topic :: topic(),
  config :: config(),
  init_flag = false :: boolean(),
  rediscover_client_tref = false :: reference(),
  workers = #{} :: #{{topic(), partition()} => pid()}
}).

%%%_* APIs =====================================================================

-spec start_link(wolff:client_id(), topic(), config()) -> {ok, pid()} | {error, {already_started, pid()}} | {error, any()}.
start_link(ClientId, Topic, Config) ->
  ?LOG(info, "start_link...~n ClientId: ~p~n Topic: ~p~n Config: ~p", [ClientId, Topic, Config]),
  Name = get_name(Config),
  gen_server:start_link({local, Name}, wolff_consumers, {ClientId, Topic, Config}, []).

-spec find_consumer(pid(), topic(), partition()) -> {ok, pid()}.
find_consumer(Pid, Topic, Partition) ->
  ?LOG(info, "find_consumer...~n Pid:~p~n Topic:~p~n Partition:~p~n", [Pid, Topic, Partition]),
  gen_server:call(Pid, {get_consumer, Topic, Partition}, infinity).

%%%_* Supervisor Callback APIs =================================================

init({ClientId, Topic, Config}) ->
  %% 获取分区数量，启动N个consumer worker进程
  ?LOG(info, "init..."),
  erlang:process_flag(trap_exit, true),
  self() ! rediscover_client,
  {ok, #state{
    client_id = ClientId,
    client_pid = false,
    topic = Topic,
    config = Config
  }}.

handle_info(rediscover_client, #state{client_id = ClientId, client_pid = false} = State) ->
  ?LOG(info, "handle_info<<rediscover_client>>...~n State:~p~n", [State]),
  State1 = State#state{rediscover_client_tref = false},
  case wolff_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      %% 监听wolff_client进程
      _ = erlang:monitor(process, Pid),
      State2 = State1#state{client_pid = Pid},
      State3 = maybe_init_consumers(State2),
      {noreply, State3};
    {error, Reason} ->
      ?LOG(info, "Failed to discover client, reason = ~p~n", [Reason]),
      {noreply, ensure_rediscover_client_timer(State1)}
  end;
handle_info(init_consumers, State) ->
  ?LOG(info, "handle_info<<init_consumers>>...~n State:~p~n", [State]),
  {noreply, maybe_init_consumers(State)};
handle_info({'DOWN', _, process, Pid, Reason}, #state{client_id = ClientId, client_pid = Pid, topic = Topic} = State) ->
  ?LOG(info, "handle info DOWN...~n Topic:~p~n ClientId:~p~n ClientPid:~p~n Reason:~p~n", [Topic, ClientId, Pid, Reason]),
  {noreply, ensure_rediscover_client_timer(State#{client_pid := false})};
handle_info(_Info, State) ->
  ?LOG(info, "handle_info...~n _Info:~p~n State:~p~n", [_Info, State]),
  {noreply, State}.

handle_call({get_consumer, Topic, Partition}, _From, #state{workers = Workers} = State) ->
  ?LOG(info, "handle_call<<get_consumer>>...~n Topic:~p~n Partition:~p~n State:~p~n", [Topic, Partition, State]),
  case maps:get({Topic, Partition}, Workers, ?undef) of
    ?undef -> {reply, {error, not_found_consumer_pid}, State};
    Pid -> {reply, {ok, Pid}, State}
  end.

code_change(_OldVsn, State, _Extra) ->
  ?LOG(info, "code change..."),
  {ok, State}.

terminate(_Reason, _State) ->
  ?LOG(info, "terminate... Reason:~p~n State: ~p~n", [_Reason, _State]),
  ok.
%%%_* Internal Callback APIs =================================================

maybe_init_consumers(#state{init_flag = false, client_id = ClientId, client_pid = ClientPid, topic = Topic, config = Config} = State) ->
  case wolff_client:get_partitions_count(ClientPid, Topic) of
    {ok, PartitionsCnt} ->
      Workers = lists:foldl(
        fun(Partition, Acc) ->
          {ok, ConsumerPid} = wolff_consumer:start_link(ClientId, Topic, Partition, Config),
          Acc#{{Topic, Partition} => ConsumerPid}
        end, #{}, lists:seq(0, PartitionsCnt - 1)),
      State#state{workers = Workers, init_flag = true};
    _ ->
      ?LOG(warning, "get partitions count error..."),
      erlang:send_after(1000, self(), init_consumers),
      State
  end;
maybe_init_consumers(State) -> State.

ensure_rediscover_client_timer(#state{rediscover_client_tref = false} = State) ->
  TimerRef = erlang:send_after(1000, self(), rediscover_client),
  State#{rediscover_client_tref := TimerRef}.


get_name(Config) ->
  maps:get(name, Config, wolff_consumers).