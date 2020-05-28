%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. 5月 2020 下午2:58
%%%-------------------------------------------------------------------
-module(wolff_subscriber_koc).
-author("zxb").

-behaviour(wolff_group_subscriber).

-include("wolff.hrl").

%% API
-export([
  bootstrap/0,
  bootstrap/1
]).

-export([
  init/2,
  handle_message/4
]).

-export([
  message_handler_loop/3
]).

-define(PRODUCE_DELAY_SECONDS, 5).

-record(callback_state, {
  handlers = [] :: [{{wolff:topic(), wolff:partition()}, pid()}],
  message_type = message :: message | message_set,
  client_id :: wolff:client_id()
}).

-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(<<"wolff-demo-group-subscriber-koc-client-1">>).

-spec bootstrap(wolff:client_id()) -> ok.
bootstrap(ClientId) ->
  emqx_logger:set_log_level(warning),
  BootstrapHosts = [{"47.95.223.12", 9092}],
  Topic = <<"wolff-demo-group-subscriber-koc">>,
  {ok, _} = application:ensure_all_started(wolff),
  %% A group ID is to be shared between the members (which often run in
  %% different Erlang nodes or even hosts).
  GroupId = <<"wolff-demo-group-subscriber-koc-consumer-group">>,
  TopicSet = [Topic],
  MemberClients = case erlang:size(ClientId) =:= 0 of
                    true ->
                      [<<"wolff-demo-group-subscriber-koc-client-1">>];
                    false ->
                      [ClientId]
                  end,
  ok = bootstrap_subscribers(MemberClients, BootstrapHosts, GroupId, TopicSet, message).

bootstrap_subscribers([], _BootstrapHosts, _GroupId, _Topics, _MsgType) -> ok;
bootstrap_subscribers([ClientId | Rest], BootstrapHosts, GroupId, Topics, MessageType) ->
  {ok, _ClientPid} = wolff:ensure_supervised_client(ClientId, BootstrapHosts, #{}),
  %% commit offsets to kafka every 5 seconds
  Config = #{
    coordinator => #{
      offset_commit_policy => commit_to_kafka_v2,
      offset_commit_interval_seconds => 1
    },
    consumer => #{
      begin_offset => earliest
    }
  },

  {ok, _Subscriber} = wolff:start_link_group_subscriber(
    ClientId, GroupId, Topics, Config, MessageType, ?MODULE, {ClientId, Topics, MessageType}),
  bootstrap_subscribers(Rest, BootstrapHosts, GroupId, Topics, MessageType).

%% @doc Initialize nothing in our case.
init(_GroupId, _CallbackInitArg = {ClientId, Topics, MessageType}) ->
  ?LOG(info, "Module: ~p~n init...~n GroupId:~p~n _CallbackInitArg:~p~n", [?MODULE, _GroupId, _CallbackInitArg]),
  Handlers = spawn_message_handlers(ClientId, Topics),
  {ok, #callback_state{handlers = Handlers, message_type = MessageType, client_id = ClientId}}.

handle_message(Topic, Partition, #kafka_message{} = Message, #callback_state{handlers = Handlers, message_type = message} = State) ->
  ?LOG(warning, "handle_message<<message>>...~n Topic~p~n Partition:~p~n Message:~p~n", [Topic, Partition, Message]),
  process_message(Topic, Partition, Handlers, Message),
  {ok, State};
handle_message(Topic, Partition, #kafka_message_set{messages = Messages} = _MessageSet,
    #callback_state{handlers = Handlers, message_type = message_set} = State) ->
  ?LOG(warning, "handle_message<<message_set>>...~n Topic~p~n Partition:~p~n Message:~p~n", [Topic, Partition, Messages]),
  [process_message(Topic, Partition, Handlers, Message) || Message <- Messages],
  {ok, State}.

%% 内部方法

process_message(Topic, Partition, Handlers, Message) ->
  %% send to a worker process
  {_, Pid} = lists:keyfind({Topic, Partition}, 1, Handlers),
  Pid ! Message.

-spec spawn_message_handlers(wolff:client_id(), [wolff:topic()]) -> [{{wolff:topic(), wolff:partition()}, pid()}].
spawn_message_handlers(_ClientId, []) -> [];
spawn_message_handlers(ClientId, [Topic | Rest]) ->
  ?LOG(info, "spawn_message_handlers... ClientId:~p Topic:~p~n", [ClientId, Topic]),
  {ok, PartitionCount} = wolff:get_partitions_count(ClientId, Topic),
  [{{Topic, Partition}, spawn_link(?MODULE, message_handler_loop, [Topic, Partition, self()])}
    || Partition <- lists:seq(0, PartitionCount - 1)] ++ spawn_message_handlers(ClientId, Rest).

message_handler_loop(Topic, Partition, SubscriberPid) ->
  receive
    #kafka_message{
      offset = Offset,
      value = Value
    } ->
      Seqno = list_to_integer(binary_to_list(Value)),
      Now = os_time_utc_str(),
      ?LOG(info, "~p ~s-~p ~s: offset:~w seqno:~w\n", [self(), Topic, Partition, Now, Offset, Seqno]),
      wolff_group_subscriber:ack(SubscriberPid, Topic, Partition, Offset),
      ?MODULE:message_handler_loop(Topic, Partition, SubscriberPid)
  after 1000 ->
    ?MODULE:message_handler_loop(Topic, Partition, SubscriberPid)
  end.

-spec os_time_utc_str() -> string().
os_time_utc_str() ->
  Ts = os:timestamp(),
  {{Y, M, D}, {H, Min, Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).