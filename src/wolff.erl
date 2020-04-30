-module(wolff).

-include("wolff.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff]").

-export_type([client_id/0, host/0, name/0, producers/0, partitioner/0, msg/0, ack_fun/0]).

-export([ensure_supervised_client/3, stop_and_delete_supervised_client/1, start_producers/3, stop_producers/1, ensure_supervised_producers/3, stop_and_delete_supervised_producers/1, send/3, send_sync/3, get_producer/2]).

-type client_id() :: binary().

-type host() :: kpro:endpoint().

-type topic() :: kpro:topic().

-type partition() :: kpro:partition().

-type name() :: atom().

-type offset() :: kpro:offset().

-type producer_cfg() :: wolff_producer:config().

-type producers() :: wolff_producers:producers().

-type partitioner() :: wolff_producers:partitioner().

-type msg() :: #{key := binary(), value := binary(), ts => pos_integer(), headers => [{binary(), binary()}]}.

-type ack_fun() :: fun((partition(), offset()) -> ok).

-spec ensure_supervised_client(ClientId, Hosts, Config) -> {'ok', pid()} | {'error', any()} when
  ClientId :: client_id(),
  Hosts :: [host()],
  Config :: wolff_client:config().
ensure_supervised_client(ClientId, Hosts, Config) ->
  ?LOG(info, " ensure supervised client...~n ClientId: ~p~n Hosts: ~p~n Config: ~p", [ClientId, Hosts, Config]),
  wolff_client_sup:ensure_present(ClientId, Hosts, Config).

-spec stop_and_delete_supervised_client(ClientId) -> ok when
  ClientId :: client_id().
stop_and_delete_supervised_client(ClientId) ->
  ?LOG(info, "stop and delete supervised client...~n ClientId: ~p", [ClientId]),
  wolff_client_sup:ensure_absence(ClientId).

-spec start_producers(Client, Topic, ProducerCfg) -> {ok, producers()} | {error, any()} when
  Client :: client_id() | pid(),
  Topic :: topic(),
  ProducerCfg :: producer_cfg().
start_producers(Client, Topic, ProducerCfg) when is_pid(Client) ->
  ?LOG(info, "start producers...~n Client: ~p~n Topic: ~p~n ProducerCfg: ~p", [Client, Topic, ProducerCfg]),
  wolff_producers:start_linked_producers(Client, Topic, ProducerCfg).

-spec stop_producers(#{workers := map(), _ => _}) -> ok.
stop_producers(Producers) ->
  ?LOG(info, "stop producers...~n Produces: ~p", [Producers]),
  wolff_producers:stop_linked(Producers).

-spec ensure_supervised_producers(client_id(), topic(),
    producer_cfg()) -> {ok, producers()} | {error, any()}.
ensure_supervised_producers(ClientId, Topic, ProducerCfg) ->
  ?LOG(info, "ensure supervised producers...~n ClientId: ~p~n Topic: ~p~n ProducerCfg: ~p", [ClientId, Topic, ProducerCfg]),
  wolff_producers:start_supervised(ClientId, Topic, ProducerCfg).

-spec stop_and_delete_supervised_producers(#{client := client_id(), topic := topic(), _ => _}) -> ok |{error, any()}.
stop_and_delete_supervised_producers(Producers) ->
  ?LOG(info, "stop producers...~n Produces: ~p", [Producers]),
  wolff_producers:stop_supervised(Producers).

-spec send(producers(), [msg()], ack_fun()) -> {partition(), pid()}.
send(Producers, Batch, AckFun) ->
  ?LOG(info, "send...~n Producers: ~p~n Batch: ~p~n AckFun: ~p", [Producers, Batch, AckFun]),
  {Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  ok = wolff_producer:send(ProducerPid, Batch, AckFun),
  {Partition, ProducerPid}.

-spec send_sync(producers(), [msg()], timeout()) -> {partition(), offset()}.
send_sync(Producers, Batch, Timeout) ->
  ?LOG(info, "send sync...~n Producers: ~p~n Batch: ~p~n Timeout: ~p", [Producers, Batch, Timeout]),
  {_Partition, ProducerPid} = wolff_producers:pick_producer(Producers, Batch),
  wolff_producer:send_sync(ProducerPid, Batch, Timeout).

-spec get_producer(producers(), partition()) -> pid().
get_producer(Producers, Partition) ->
  ?LOG(info, "get producer...~n Producers: ~p~n Partition: ~p", [Producers, Partition]),
  wolff_producers:lookup_producer(Producers, Partition).