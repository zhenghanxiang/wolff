%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. 4月 2020 下午3:16
%%%-------------------------------------------------------------------
-module(wolff_client).
-author("zxb").

-behaviour(gen_server).

-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").
-include_lib("kafka_protocol/include/kpro_error_codes.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff client]").

%% API
-export([start_link/3, stop/1, get_id/1, get_leader_connections/2, recv_leader_connection/4]).

-export([init/1, handle_call/3, handle_info/2, handle_cast/2, code_change/3, terminate/2]).

-export_type([config/0, state/0]).

-type config() :: map().

-type topic() :: kpro:topic().

-type partition() :: kpro:partition().

-type connection() :: kpro:connection().

-type host() :: kpro:host().

-type conn_id() :: {topic(), partition()} | host().

-type state() :: #{client_id := wolff:client_id(),
seed_hosts := host(),
config := config(),
connect := fun((host()) -> {ok, connection()} | {error, any()}),
conns := #{conn_id() => connection()},
metadata_ts := #{topic() => erlang:timestamp()},
leaders => #{{topic(), partition()} => connection()}
}.

%% 对外接口
-spec start_link(ClientId, Hosts, Config) -> {'ok', pid()} | {'error', any()} when
  ClientId :: wolff:client_id(),
  Hosts :: [host()],
  Config :: config().
start_link(ClientId, Hosts, Config) ->
  ?LOG(info, "start link...~n ClientId: ~p~n Hosts: ~p~n Config: ~p", [ClientId, Hosts, Config]),
  {ConnectCfg, MyCfg} = split_config(Config),
  ConnCfg = ConnectCfg#{client_id => ClientId},
  State = #{
    client_id => ClientId,
    seed_hosts => Hosts,
    config => MyCfg,
    connect => fun(Host) -> kpro:connect(Host, ConnCfg) end,
    conns => #{},
    metadata_ts => #{},
    leaders => #{}
  },
  case maps:get(reg_name, Config, false) of
    false -> gen_server:start_link(wolff_client, State, []);
    Name -> gen_server:start_link({local, Name}, wolff_client, State, [])
  end.

-spec stop(Pid) -> Reply when
  Pid :: pid(),
  Reply :: term().
stop(Pid) ->
  ?LOG(info, "stop...~n Pid: ~p", [Pid]),
  gen_server:call(Pid, stop, infinity).

-spec get_id(Pid) -> Reply when
  Pid :: pid(),
  Reply :: term().
get_id(Pid) ->
  ?LOG(info, "get_id...~n Pid: ~p", [Pid]),
  gen_server:call(Pid, get_id, infinity).

-spec get_leader_connections(ClientPid, Topic) -> Reply when
  ClientPid :: pid(),
  Topic :: topic(),
  Reply :: term().
get_leader_connections(ClientPid, Topic) ->
  ?LOG(info, "get_leader_connections...~n ClientPid: ~p~n Topic: ~p", [ClientPid, Topic]),
  gen_server:call(ClientPid, {get_leader_connections, Topic}, infinity).

-spec recv_leader_connection(Client, Topic, Partition, Pid) -> 'ok' when
  Client :: term(),
  Topic :: topic(),
  Partition :: partition(),
  Pid :: pid().
recv_leader_connection(Client, Topic, Partition, Pid) ->
  ?LOG(info, "recv_leader_connection...~n Client: ~p~n Topic: ~p~n Partition: ~p~n Pid: ~p", [Client, Topic, Partition, Pid]),
  gen_server:cast(Client, {recv_leader_connection, Topic, Partition, Pid}).

%% 回调接口
init(State) ->
  ?LOG(info, "init...~n State: ~p", [State]),
  erlang:process_flag(trap_exit, true),
  {ok, State}.

handle_call(stop, From, #{conns := Connections} = State) ->
  ?LOG(info, "handle_call stop...~n State: ~p", [State]),
  ok = close_connections(Connections),
  gen_server:reply(From, ok),
  {stop, normal, State#{conns := #{}}};
handle_call(get_id, _From, #{client_id := Id} = State) ->
  ?LOG(info, "handle_call get_id...~n State: ~p", [State]),
  {reply, Id, State};
handle_call({get_leader_connections, Topic}, _From, State) ->
  ?LOG(info, "handle_call get_leader_connections...~n Topic: ~p~n State: ~p", [Topic, State]),
  case ensure_leader_connections(State, Topic) of
    {ok, NewState} ->
      Result = do_get_leader_connections(NewState, Topic),
      ?LOG(info, "do get leader connections result: ~p", [Result]),
      {reply, {ok, Result}, NewState};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call(_Req, _From, State) -> {noreply, State}.

handle_info(_Info, State) ->
  ?LOG(info, "handle_info...~n _Info: ~p~n State: ~p", [_Info, State]),
  {noreply, State}.

handle_cast({recv_leader_connection, Topic, Partition, Pid}, State) ->
  ?LOG(info, "handle_cast recv_leader_connection...~n Topic: ~p~n Partition: ~p~n Pid: ~p~n State: ~p", [Topic, Partition, Pid, State]),
  case ensure_leader_connections(State, Topic) of
    {ok, NewState} ->
      Partitions = do_get_leader_connections(NewState, Topic),
      {_, MaybePid} = lists:keyfind(Partition, 1, Partitions),
      _ = erlang:send(Pid, {leader_connection, MaybePid}),
      {noreply, NewState};
    {error, Reason} ->
      _ = erlang:send(Pid, {leader_connection, {error, Reason}}),
      {noreply, State}
  end;
handle_cast(_Req, State) ->
  ?LOG(info, "handle_cast...~n _Req: ~p~n State: ~p", [_Req, State]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_, #{conns := Connections} = State) ->
  ?LOG(info, "terminate...~n State: ~p", [State]),
  ok = close_connections(Connections),
  {ok, State#{conns := #{}}}.

%% 内部调用
split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun({K, _V}) -> lists:member(K, ConnCfgKeys) end,
  {ConnCfg, MyCfg} = lists:partition(Pred, maps:to_list(Config)),
  {maps:from_list(ConnCfg), maps:from_list(MyCfg)}.

close_connections(Connections) ->
  lists:foreach(fun({_, Pid}) -> close_connection(Pid) end, maps:to_list(Connections)).

close_connection(Conn) ->
  _ = spawn(fun() -> do_close_connection(Conn) end),
  ok.

do_close_connection(Pid) ->
  ?LOG(info, "do close connection Pid: ~p", [Pid]),
  MonitorRef = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), MonitorRef}, stop}),
  receive
    {MonitorRef, Reply} ->
      erlang:demonitor(MonitorRef, [flush]),
      Reply;
    {'DOWN', MonitorRef, _, _, Reason} ->
      {error, {connection_down, Reason}}
  after 5000 ->
    ?LOG(info, "do close connection timeout... force kill process"),
    exit(Pid, kill)
  end.

ensure_leader_connections(State, Topic) ->
  ?LOG(info, "ensure leader connections...~n State: ~p~n Topic: ~p", [State, Topic]),
  case is_metadata_fresh(State, Topic) of
    true -> {ok, State};
    false -> do_ensure_leader_connections(State, Topic)
  end.

is_metadata_fresh(#{metadata_ts := MetadataTs, config := Config}, Topic) ->
  MinInterval = maps:get(min_metadata_refresh_interval, Config, 1000),
  case maps:get(Topic, MetadataTs, false) of
    false -> false;
    Ts -> timer:now_diff(erlang:timestamp(), Ts) < MinInterval * 1000
  end.

do_ensure_leader_connections(#{connect := ConnectFun,
  seed_hosts := SeedHosts, metadata_ts := MetadataTs} = State, Topic) ->
  case get_metadata(SeedHosts, ConnectFun, Topic, []) of
    {ok, {Brokers, P_Metadata}} ->
      NewState = lists:foldl(
        fun(P_Meta, StateIn) ->
          try
            ensure_leader_connection(StateIn, Brokers, Topic, P_Meta)
          catch
            error : Reason ->
              ?LOG(info, "Bad metadata for ~p-~p, Reason=~p", [Topic, P_Meta, Reason]),
              StateIn
          end
        end, State, P_Metadata),
      {ok, NewState#{metadata_ts := MetadataTs#{Topic => erlang:timestamp()}}};
    {error, Reason} ->
      ?LOG(info, "Failed to get metadata! Reason: ~p", [Reason]),
      {error, failed_to_fetch_metadata}
  end.

ensure_leader_connection(#{connect := ConnectFun, conns := Connections} = State, Brokers, Topic, P_Meta) ->
  ?LOG(info, "ensure leader connection...~n Brokers: ~p~n Topic: ~p~n P_Meta: ~p~n State: ~p", [Brokers, Topic, P_Meta, State]),
  Leaders = maps:get(leaders, State, #{}),
  ErrorCode = kpro:find(error_code, P_Meta),
  ErrorCode =:= no_error orelse erlang:error(ErrorCode),
  P_Idx = kpro:find(partition, P_Meta),
  LeaderBrokerId = kpro:find(leader, P_Meta),
  {_, Host} = lists:keyfind(LeaderBrokerId, 1, Brokers),
  Strategy = get_connection_strategy(State),
  ConnId = case Strategy of
             per_partition -> {Topic, P_Idx};
             per_broker -> Host
           end,
  NewConnections = case get_connected(ConnId, Host, Connections) of
                     already_connected -> Connections;
                     {needs_reconnect, OldConn} ->
                       ok = close_connection(OldConn),
                       add_conn(ConnectFun(Host), ConnId, Connections);
                     false ->
                       add_conn(ConnectFun(Host), ConnId, Connections)
                   end,
  NewState = State#{conns := NewConnections},
  case Strategy of
    per_broker ->
      NewLeaders = Leaders#{{Topic, P_Idx} => maps:get(ConnId, NewConnections)},
      NewState#{leaders => NewLeaders};
    _ -> NewState
  end.

get_metadata([], _ConnectFun, _Topic, Errors) ->
  ?LOG(info, "get metadata errors: ~p", [Errors]),
  {error, Errors};
get_metadata([Host | Rest], ConnectFun, Topic, Errors) ->
  case ConnectFun(Host) of
    {ok, Pid} ->
      ?LOG(info, "get metadata ConnectFun Pid: ~p", [Pid]),
      try
        {ok, Versions} = kpro:get_api_versions(Pid),
        ?LOG(info, "Versions: ~p", [Versions]),
        {_, Vsn} = maps:get(metadata, Versions),
        do_get_metadata(Vsn, Pid, Topic)
      after
        ?LOG(info, "close connection"),
        _ = close_connection(Pid)
      end;
    {error, Reason} ->
      get_metadata(Rest, ConnectFun, Topic, [{Host, Reason} | Errors])
  end.

do_get_metadata(Vsn, Pid, Topic) ->
  ?LOG(info, "do get metadata...~n Vsn: ~p~n Pid: ~p~n Topic: ~p", [Vsn, Pid, Topic]),
  Req = kpro:make_request(metadata, Vsn, [{topics, [Topic]}, {allow_auto_topic_creation, false}]),
  ?LOG(info, "do_get_metadata Req: ~p", [Req]),
  case kpro:request_sync(Pid, Req, 10000) of
    {ok, #kpro_rsp{msg = Meta}} ->
      ?LOG(info, "Meta: ~p", [Meta]),
      BrokersMeta = kpro:find(brokers, Meta),
      ?LOG(info, "BrokersMeta: ~p", [BrokersMeta]),
      Brokers = [parse_broker_meta(M) || M <- BrokersMeta],
      [TopicMeta] = kpro:find(topic_metadata, Meta),
      ErrorCode = kpro:find(error_code, TopicMeta),
      P_Metadata = kpro:find(partition_metadata, TopicMeta),
      case ErrorCode =:= no_error of
        true -> {ok, {Brokers, P_Metadata}};
        false -> {error, ErrorCode}
      end;
    {error, Reason} ->
      ?LOG(info, "do_get_metadata error: ~p", [Reason]),
      {error, Reason}
  end.

parse_broker_meta(BrokerMeta) ->
  ?LOG(info, "parse broker meta BrokerMeta: ~p", [BrokerMeta]),
  BrokerId = kpro:find(node_id, BrokerMeta),
  Host = kpro:find(host, BrokerMeta),
  Port = kpro:find(port, BrokerMeta),
  {BrokerId, {Host, Port}}.

get_connection_strategy(#{config := Config}) ->
  maps:get(connection_strategy, Config, per_partition).

get_connected(Host, Host, Connections) ->
  Pid = maps:get(Host, Connections, false),
  is_alive(Pid) andalso already_connected;
get_connected(ConnId, Host, Connections) ->
  Pid = maps:get(ConnId, Connections, false),
  case is_connected(Pid, Host) of
    true -> already_connected;
    false when is_pid(Pid) ->
      ?LOG(info, "get connected needs reconnect..."),
      {needs_reconnect, Pid};
    false -> false
  end.

is_alive(Pid) ->
  is_pid(Pid) andalso erlang:is_process_alive(Pid).

is_connected(MaybePid, {Host, _Port}) ->
  is_alive(MaybePid) andalso
    case kpro_connection:get_endpoint(MaybePid) of
      {ok, {H, _P}} ->
        iolist_to_binary(Host) =:= iolist_to_binary(H);
      _ -> false
    end.

add_conn({ok, Pid}, ConnId, Connections) ->
  Connections#{ConnId => Pid};
add_conn({error, Reason}, ConnId, Connections) ->
  Connections#{ConnId => Reason}.

do_get_leader_connections(#{conns := Connections} = State, Topic) ->
  ?LOG(info, "do get leader connections...~n State: ~p~n Topic: ~p", [State, Topic]),
  FindInMap =
    case get_connection_strategy(State) of
      per_partition -> Connections;
      per_broker -> maps:get(leaders, State)
    end,

  F =
    fun({T, P}, MaybePid, Acc) when T =:= Topic ->
      case is_alive(MaybePid) of
        true -> [{P, MaybePid} | Acc];
        false -> [{P, {down, MaybePid}} | Acc]
      end;
      (_, _, Acc) -> Acc
    end,
  maps:fold(F, [], FindInMap).