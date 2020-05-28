%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. 4月 2020 下午2:24
%%%-------------------------------------------------------------------
-module(wolff_producer).
-author("zxb").

-behaviour(gen_server).

-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff producer]").

%% API
-export([start_link/5, stop/1, send/3, send_sync/3]).

-export([queue_item_sizer/1, queue_item_marshaller/1]).

-export([init/1, handle_call/3, handle_info/2, handle_cast/2, code_change/3, terminate/2]).

-export_type([config/0]).

-type topic() :: kpro:topic().

-type partition() :: kpro:partition().

-type offset() :: kpro:offset().

-type config() :: #{replayq_dir := string(),
replayq_seg_bytes => pos_integer(),
required_acks => kpro:required_acks(),
ack_timeout => timeout(),
max_batch_bytes => pos_integer(),
min_batch_bytes => pos_integer(),
max_linger_ms => non_neg_integer(),
max_send_ahead => non_neg_integer(),
compression => kpro:compress_option()
}.

%% 对外接口
-spec start_link(wolff:client_id(), topic(), partition(), pid() | {down, any()}, config()) ->
  {ok, pid()} | {error, any()}.
start_link(ClientId, Topic, Partition, MaybeConnPid, Config) ->
  ?LOG(info, "start link...~n ClientId: ~p~n Topic: ~p~n Partition: ~p~n MaybeConnPid: ~p~n Config: ~p",
    [ClientId, Topic, Partition, MaybeConnPid, Config]),
  State = #{
    client_id => ClientId,
    topic => Topic,
    partition => Partition,
    conn => MaybeConnPid,
    config => use_defaults(Config)
  },
  gen_server:start_link(wolff_producer, State, []).

-spec stop(pid()) -> any().
stop(Pid) ->
  ?LOG(info, "stop... Pid: ~p", [Pid]),
  gen_server:call(Pid, stop, infinity).

-spec send(pid(), [wolff:msg()], wolff:ack_fun()) -> ok.
send(Pid, [_ | _] = Batch, AckFun) ->
  ?LOG(info, "send...~n Pid: ~p~n Batch: ~p~n AckFun: ~p", [Pid, Batch, AckFun]),
  Caller = self(),
  MonitorRef = erlang:monitor(process, Pid),
  NewBatch = ensure_ts(Batch),
  erlang:send(Pid, {send, {Caller, MonitorRef}, NewBatch, AckFun}),
  receive
    {MonitorRef, queued} ->
      erlang:demonitor(MonitorRef, [flush]),
      ok;
    {'DOWN', MonitorRef, _, _, Reason} -> erlang:error({producer_down, Reason})
  end.

-spec send_sync(pid(), [wolff:msg()], timeout()) -> {partition(), offset()}.
send_sync(Pid, Batch, Timeout) ->
  ?LOG(info, "send sync...~n Pid: ~p~n Batch: ~p~n Timeout: ~p", [Pid, Batch, Timeout]),
  Caller = self(),
  MonitorRef = erlang:monitor(process, Pid),
  AckFun =
    fun(Partition, BaseOffset) ->
      _ = erlang:send(Caller, {MonitorRef, Partition, BaseOffset}),
      ok
    end,
  NewBatch = ensure_ts(Batch),
  erlang:send(Pid, {send, no_queued_reply, NewBatch, AckFun}),
  receive
    {MonitorRef, Partition, BaseOffset} ->
      ?LOG(info, "send sync success...~n Pid: ~p~n Partition: ~p~n BaseOffset: ~p", [Pid, Partition, BaseOffset]),
      erlang:demonitor(MonitorRef, [flush]),
      {Partition, BaseOffset};
    {'DOWN', MonitorRef, _, _, Reason} ->
      ?LOG(error, "send sync fail...~n Pid: ~p~n Reason: ~p", [Pid, Reason]),
      erlang:error({producer_down, Reason})
  after Timeout ->
    erlang:demonitor(MonitorRef, [flush]),
    erlang:error(timeout)
  end.

%% 回调接口
init(State) ->
  ?LOG(info, "init... State: ~p", [State]),
  erlang:process_flag(trap_exit, true),
  self() ! {do_init, State},
  {ok, #{}}.

handle_call(stop, From, State) ->
  ?LOG(info, "handle call stop... State: ~p", [State]),
  gen_server:reply(From, ok),
  {stop, normal, State};
handle_call(_Req, _From, State) ->
  ?LOG(info, "handle call...~n _Req: ~p~n State: ~p", [_Req, State]),
  {noreply, State}.

handle_info({send, _, Batch, _} = Req,
    #{client_id := ClientId, topic := Topic, partition := Partition,
      config := #{max_batch_bytes := Limit}} = State) ->
  ?LOG(info, "handle info send...~n Req: ~p~n State: ~p", [Req, State]),
  {Calls, Cnt, Oct} = collect_send_calls([Req], 1, batch_size(Batch), Limit),
  ok = wolff_stats:recv(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}),
  NewState = maybe_send_to_kafka(enqueue_calls(Calls, State)),
  {noreply, NewState};
handle_info({do_init, State}, _) ->
  ?LOG(info, "handle info do_init... State: ~p", [State]),
  NewState = do_init(State),
  {noreply, NewState};
handle_info(linger_expire, State) ->
  ?LOG(info, "handle info linger_expire... State: ~p", [State]),
  {noreply, maybe_send_to_kafka(State)};
handle_info({msg, Conn, Rsp}, #{conn := Conn} = State) ->
  ?LOG(info, "handle info msg...~n Conn: ~p~n Rsp: ~p~n State: ~p", [Conn, Rsp, State]),
  try handle_kafka_ack(Rsp, State) of
    TempState -> NewState = maybe_send_to_kafka(TempState),
      {noreply, NewState}
  catch Reason ->
    NewState = mark_connection_down(State, Reason),
    {noreply, NewState}
  end;
handle_info({leader_connection, Conn}, State) when is_pid(Conn) ->
  ?LOG(info, "handle info leader_connection...~n Conn: ~p~n State: ~p", [Conn, State]),
  _ = erlang:monitor(process, Conn),
  State1 = State#{reconnect_timer => no_timer, conn := Conn},
  State2 = get_produce_version(State1),
  State3 = resend_sent_reqs(State2),
  NewState = maybe_send_to_kafka(State3),
  {noreply, NewState};
handle_info({leader_connection, {down, Reason}}, State) ->
  ?LOG(info, "handle info leader_connection down...~n Reason: ~p~n State: ~p", [Reason, State]),
  NewState = mark_connection_down(State#{reconnect_timer => no_timer}, Reason),
  {noreply, NewState};
handle_info({leader_connection, {error, Reason}}, State) ->
  ?LOG(info, "handle info leader_connection error...~n Reason: ~p~n State: ~p", [Reason, State]),
  NewState = mark_connection_down(State#{reconnect_timer => no_timer}, Reason),
  {noreply, NewState};
handle_info(reconnect, State) ->
  ?LOG(info, "handle info reconnect...~n State: ~p", [State]),
  NewState = State#{reconnect_timer => no_timer},
  {noreply, ensure_delayed_reconnect(NewState)};
handle_info({'DOWN', _, _, Conn, Reason}, #{conn := Conn} = State) ->
  ?LOG(info, "handle info DOWN...~n Conn: ~p~n Reason: ~p~n State: ~p", [Conn, Reason, State]),
  #{reconnect_timer := no_timer} = State,
  NewState = mark_connection_down(State, Reason),
  {noreply, NewState};
handle_info({'EXIT', _, Reason}, State) ->
  ?LOG(info, "handle info EXIT...~n Reason: ~p~n State: ~p", [Reason, State]),
  {stop, Reason, State};
handle_info(_Req, State) ->
  ?LOG(info, "handle info...~n _Req: ~p~n State: ~p", [_Req, State]),
  {noreply, State}.

handle_cast(_Req, State) ->
  ?LOG(info, "handle cast...~n _Req: ~p~n State: ~p", [_Req, State]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  ?LOG(info, "code change...~n State: ~p", [State]),
  {ok, State}.

terminate(_Info, #{replayq := Q}) ->
  ?LOG(info, "terminate...~n _Info: ~p~n Q: ~p", [_Info, Q]),
  ok = replayq:close(Q);
terminate(_, _) ->
  ?LOG(info, "terminate..."),
  ok.

%% 内部接口
ensure_ts(Batch) ->
  lists:map(
    fun(#{ts := _} = Msg) -> Msg;
      (Msg) -> Msg#{ts => now_ts()}
    end, Batch).

now_ts() -> erlang:system_time(millisecond).

use_defaults(Config) ->
  use_defaults(Config,
    [
      {required_acks, all_isr}, {ack_timeout, 10000}, {max_batch_bytes, 900000}, {min_batch_bytes, 1024},
      {max_linger_ms, 0}, {max_send_ahead, 0}, {compression, no_compression}, {reconnect_delay_ms, 2000}
    ]).

use_defaults(Config, []) -> Config;
use_defaults(Config, [{K, V} | Rest]) ->
  case maps:is_key(K, Config) of
    true -> use_defaults(Config, Rest);
    false -> use_defaults(Config#{K => V}, Rest)
  end.

collect_send_calls(Calls, Count, Size, Limit) when Size >= Limit ->
  ?LOG(info, "collect send calls...~n Size: ~p~n Limit: ~p", [Size, Limit]),
  {lists:reverse(Calls), Count, Size};
collect_send_calls(Calls, Count, Size, Limit) ->
  ?LOG(info, "collect send calls...~n Calls: ~p~n Count: ~p~n Size: ~p~n Limit: ~p", [Calls, Count, Size, Limit]),
  receive
    {send, _, Batch, _} = Call ->
      ?LOG(info, "collect send calls receive...~n Call: ~p", [Call]),
      collect_send_calls([Call | Calls], Count + 1, Size + batch_size(Batch), Limit)
  after 0 ->
    ?LOG(info, "collect send calls after..."),
    {lists:reverse(Calls), Count, Size}
  end.

batch_size(Batch) ->
  lists:foldl(fun(M, Sum) -> oct(M) + Sum end, 0, Batch).

oct(#{key := K, value := V, ts := Ts} = Msg) ->
  Headers = maps:get(headers, Msg, []),
  HeadersSize = lists:foldl(fun({HK, HV}, Acc) -> Acc + size(HK) + size(HV) end, 0, Headers),
  TsSize = iolist_size(kpro_varint:encode(Ts)),
  HeadersSize + size(K) + size(V) + TsSize.

enqueue_calls(Calls, #{replayq := Q, pending_acks := PendingAcks,
  call_id_base := CallIdBase, partition := Partition} = State) ->
  {QueueItems, NewPendingAcks} = lists:foldl(
    fun({send, _From, Batch, AckFun}, {Items, PendingAcksIn}) ->
      CallId = make_call_id(CallIdBase),
      NewAckFun = fun(BaseOffset) -> AckFun(Partition, BaseOffset) end,
      PendingAcksOut = PendingAcksIn#{CallId => NewAckFun},
      NewItems = [make_queue_item(CallId, Batch) | Items],
      {NewItems, PendingAcksOut}
    end, {[], PendingAcks}, Calls),
  NewQ = replayq:append(Q, lists:reverse(QueueItems)),
  lists:foreach(fun maybe_reply_queued/1, Calls),
  State#{replayq := NewQ, pending_acks := NewPendingAcks}.

make_call_id(Base) ->
  Base + erlang:unique_integer([positive]).

make_queue_item(CallId, Batch) ->
  {CallId, now_ts(), Batch}.

maybe_reply_queued({send, no_queued_reply, _, _}) -> ok;
maybe_reply_queued({send, {Pid, Ref}, _, _}) ->
  erlang:send(Pid, {Ref, queued}).

maybe_send_to_kafka(#{conn := Conn} = State) ->
  ?LOG(info, "maybe send to kafka...~n State:~p", [State]),
  case is_idle(State) of
    true ->
      ?LOG(info, "is idle true..."),
      State;
    false when is_pid(Conn) ->
      ?LOG(info, "is idle false... Pid: ~", [Conn]),
      maybe_send_to_kafka_2(State);
    false ->
      ?LOG(info, "is idle false..."),
      ensure_delayed_reconnect(State)
  end.

maybe_send_to_kafka_2(State) ->
  ?LOG(info, "maybe send to kafka2...~n State:~p", [State]),
  LingerTimeout = first_item_expire_time(State),
  IsTimedOut = is_integer(LingerTimeout) andalso LingerTimeout =< 0,
  case is_send_ahead_allowed(State) andalso (is_queued_enough_bytes(State) orelse IsTimedOut) of
    true ->
      ?LOG(info, "allow send to kafka"),
      send_to_kafka(State);
    false ->
      case is_integer(LingerTimeout) andalso LingerTimeout > 0 of
        true -> erlang:send_after(LingerTimeout, self(), linger_expire);
        false -> ok
      end,
      State
  end.

is_idle(#{replayq := Q, sent_reqs := SentReqs}) ->
  SentReqs =:= [] andalso replayq:count(Q) =:= 0.

first_item_expire_time(#{replayq := Q, config := #{max_linger_ms := Max}}) ->
  case replayq:peek(Q) of
    empty -> false;
    Item -> Max - (now_ts() - get_item_ts(Item))
  end.

get_item_ts({_, Ts, _}) -> Ts.

is_send_ahead_allowed(#{config := #{max_send_ahead := Max}, sent_reqs := Sent}) ->
  length(Sent) - 1 < Max.

is_queued_enough_bytes(#{replayq := Q, config := #{min_batch_bytes := Min}}) ->
  Queued = replayq:bytes(Q),
  Queued > 0 andalso Queued >= Min.

send_to_kafka(#{sent_reqs := Sent, replayq := Q,
  config := #{max_batch_bytes := BytesLimit, required_acks := RequiredAcks,
    ack_timeout := AckTimeout, compression := Compression},
  conn := Conn, produce_api_vsn := Vsn, topic := Topic, partition := Partition} = State) ->
  ?LOG(info, "send to kafka...~n State: ~p", [State]),
  {NewQ, QAckRef, Items} = replayq:pop(Q, #{bytes_limit => BytesLimit, count_limit => 999999999}),
  {FlatBatch, Calls} = get_flat_batch(Items, [], []),
  [_ | _] = FlatBatch, %%
  Req = kpro_req_lib:produce(Vsn, Topic, Partition, FlatBatch, #{ack_timeout => AckTimeout,
    required_acks => RequiredAcks, compression => Compression}),

  NewSent = {Req, QAckRef, Calls},
  State1 = State#{replayq := NewQ, sent_reqs := Sent ++ [NewSent]},
  ok = request_async(Conn, Req),
  ok = send_stats(State1, FlatBatch),
  State2 = maybe_fake_kafka_ack(Req, State1),
  maybe_send_to_kafka(State2).

get_flat_batch([], Messages, Calls) ->
  {lists:reverse(Messages), lists:reverse(Calls)};
get_flat_batch([QItem | Rest], Messages, Calls) ->
  {CallId, _Ts, Batch} = QItem,
  get_flat_batch(Rest, lists:reverse(Batch, Messages), [{CallId, length(Batch)} | Calls]).

request_async(Conn, Req) when is_pid(Conn) ->
  ?LOG(info, "request async...~n Conn: ~p~n Req: ~p", [Conn, Req]),
  ok = kpro:send(Conn, Req).

send_stats(#{client_id := ClientId, topic := Topic, partition := Partition}, Batch) ->
  {Cnt, Oct} = lists:foldl(fun(Msg, {C, O}) -> {C + 1, O + oct(Msg)} end, {0, 0}, Batch),
  ok = wolff_stats:sent(ClientId, Topic, Partition, #{cnt => Cnt, oct => Oct}).

maybe_fake_kafka_ack(#kpro_req{no_ack = true, ref = Ref}, State) ->
  ?LOG(info, "maybe fake kafka ack..."),
  do_handle_kafka_ack(Ref, -1, State);
maybe_fake_kafka_ack(_Req, State) ->
  ?LOG(info, "maybe fake kafka ack...~n _Req: ~p~n State: ~p", [_Req, State]),
  State.

do_handle_kafka_ack(Ref, BaseOffset,
    #{sent_reqs := [{#kpro_req{ref = Ref}, Q_AckRef, Calls} | Rest],
      pending_acks := PendingAcks, replayq := Q} = State) ->
  ok = replayq:ack(Q, Q_AckRef),
  NewPendingAcks = evaluate_pending_ack_funs(PendingAcks, Calls, BaseOffset),
  State#{sent_reqs := Rest, pending_acks := NewPendingAcks};
do_handle_kafka_ack(_Ref, _BaseOffset, State) -> State.

evaluate_pending_ack_funs(PendingAcks, [], _BaseOffset) ->
  PendingAcks;
evaluate_pending_ack_funs(PendingAcks, [{CallId, BatchSize} | Rest], BaseOffset) ->
  NewPendingAcks =
    case maps:get(CallId, PendingAcks, false) of
      AckFun when is_function(AckFun, 1) ->
        ok = AckFun(BaseOffset),
        maps:without([CallId], PendingAcks);
      false -> PendingAcks
    end,
  evaluate_pending_ack_funs(NewPendingAcks, Rest, BaseOffset + BatchSize).

ensure_delayed_reconnect(#{config := #{reconnect_delay_ms := Delay},
  client_id := ClientId, topic := Topic, partition := Partition,
  reconnect_timer := no_timer} = State) ->
  ?LOG(info, "ensure delayed reconnect...~n State: ~p", [State]),
  NewDelay = Delay + rand:uniform(1000),
  case wolff_client_sup:find_client(ClientId) of
    {ok, ClientPid} ->
      Args = [ClientPid, Topic, Partition, self()],
      ?LOG(info, "Will try to rediscover leader connection after ~p ms delay", [NewDelay]),
      {ok, Timer} = timer:apply_after(NewDelay, wolff_client, recv_leader_connection, Args),
      State#{reconnect_timer := Timer};
    {error, _Restarting} ->
      ?LOG(info, "Will try to rediscover client pid after ~p ms delay", [NewDelay]),
      {ok, Timer} = timer:apply_after(NewDelay, erlang, send, [self(), reconnect]),
      State#{reconnect_timer := Timer}
  end;
ensure_delayed_reconnect(State) -> State.

do_init(#{client_id := ClientId, conn := Conn, topic := Topic, partition := Partition, config := Config} = State) ->
  ?LOG(info, "do init..."),
  QConfig =
    case maps:get(replayq_dir, Config, false) of
      false -> #{mem_only => true};
      BaseDir ->
        Dir = filename:join([BaseDir, Topic, integer_to_list(Partition)]),
        SegBytes = maps:get(replayq_seg_bytes, Config, 10 * 1024 * 1024),
        #{dir => Dir, seg_bytes => SegBytes}
    end,
  Q = replayq:open(QConfig#{sizer => fun wolff_producer:queue_item_sizer/1,
    marshaller => fun wolff_producer:queue_item_marshaller/1}),
  _ = erlang:send(self(), {leader_connection, Conn}),
  NewConfig = maps:without([replayq_dir, replayq_seg_bytes], Config),
  State#{replayq => Q, config := NewConfig, call_id_base => erlang:system_time(microsecond),
    pending_acks => #{}, sent_reqs => [], conn := undefined, client_id => ClientId}.

queue_item_sizer({_CallId, _Ts, Batch}) ->
  batch_size(Batch).

queue_item_marshaller({_, _, _} = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  binary_to_term(Bin).

handle_kafka_ack(#kpro_rsp{api = produce, ref = Ref, msg = Rsp}, State) ->
  [TopicRsp] = kpro:find(responses, Rsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  ErrorCode = kpro:find(error_code, PartitionRsp),
  BaseOffset = kpro:find(base_offset, PartitionRsp),
  case ErrorCode =:= no_error of
    true -> do_handle_kafka_ack(Ref, BaseOffset, State);
    false ->
      #{topic := Topic, partition := Partition} = State,
      ?LOG(warning, "~s-~p: Produce response error-code = ~p", [Topic, Partition, ErrorCode]),
      erlang:throw(ErrorCode)
  end.

mark_connection_down(#{topic := Topic, partition := Partition, conn := Old} = State, Reason) ->
  NewState = State#{conn := Reason},
  case is_idle(NewState) of
    true -> NewState;
    false ->
      maybe_log_connection_down(Topic, Partition, Old, Reason),
      ensure_delayed_reconnect(NewState)
  end.

maybe_log_connection_down(_Topic, _Partition, _, to_be_discovered) ->
  ok;
maybe_log_connection_down(Topic, Partition, Conn, Reason) when is_pid(Conn) ->
  ?LOG(warning, "Producer ~s-~p: Connection ~p down. Reason: ~p", [Topic, Partition, Conn, Reason]);
maybe_log_connection_down(Topic, Partition, _, Reason) ->
  ?LOG(warning, "Producer ~s-~p: Failed to reconnect. Reason: ~p", [Topic, Partition, Reason]).

get_produce_version(#{conn := Conn} = State) when is_pid(Conn) ->
  ?LOG(info, "get produce version...~n State: ~p", [State]),
  Vsn = case kpro:get_api_vsn_range(Conn, produce) of
          {ok, {_Min, Max}} -> Max;
          {error, _} -> 3
        end,
  State#{produce_api_vsn => Vsn}.

resend_sent_reqs(#{sent_reqs := []} = State) ->
  ?LOG(info, "resend sent reqs 1..."),
  State;
resend_sent_reqs(#{sent_reqs := SentReqs, conn := Conn} = State) ->
  ?LOG(info, "resend sent reqs 2..."),
  F =
    fun({ReqIn, Q_AckRef, Calls}, Acc) ->
      Req = ReqIn#kpro_req{ref = make_ref()},
      ok = request_async(Conn, Req),
      NewState = {Req, Q_AckRef, Calls},
      Acc ++ [NewState]
    end,
  NewSentReqs = lists:foldl(F, [], SentReqs),
  State#{sent_reqs := NewSentReqs}.