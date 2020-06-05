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

-include("wolff.hrl").

-logger_header("[wolff client]").

-import(wolff_utils, [kf/2]).

%% API
-export([
  start_link/3,
  stop/1,
  get_id/1,
  get_leader_connections/2,
  get_leader_connection/3,
  recv_leader_connection/4,
  get_group_coordinator/2,
  get_partitions_count/2,
  get_consumer/3,
  register_consumer/4,
  deregister_consumer/3
]).

-export([
  init/1,
  handle_call/3,
  handle_info/2,
  handle_cast/2,
  code_change/3,
  terminate/2
]).

-export_type([config/0, state/0]).

%% ClientId as ets table name.
-define(ETS(ClientId), wolff_utils:ensure_atom(ClientId)).
-define(DEFAULT_GET_METADATA_TIMEOUT_SECONDS, 5).
-define(CONSUMER_KEY(Topic, Partition), {consumer, Topic, Partition}).
-define(CONSUMER(Topic, Partition, Pid), {?CONSUMER_KEY(Topic, Partition), Pid}).


-type client() :: wolff:client().
-type config() :: map().
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type connection() :: kpro:connection().
-type host() :: kpro:host().
-type conn_id() :: {topic(), partition()} | host().
-type get_consumer_error() :: client_down | {consumer_down, noproc} | {consumer_not_found, topic()} | {consumer_not_found, topic(), partition()}.


-record(state, {
  client_id :: wolff:client_id(),
  seed_hosts :: host(),
  conn_config :: config(),
  custom_config :: config(),
  meta_conn :: ?undef | connection(),
  payload_connections :: #{conn_id() => connection()},
  metadata = #{} :: #{topic() => kpro:struct()} | #{},
  metadata_ts = #{} :: #{topic() => erlang:timestamp()},
  leaders = #{} :: #{{topic(), partition()} => connection()},
  workers_tab :: ?undef | ets:tab()
}).

-type state() :: #state{}.

%% 对外接口
-spec start_link(ClientId, Hosts, Config) -> {'ok', pid()} | {'error', any()} when
  ClientId :: wolff:client_id(),
  Hosts :: [host()],
  Config :: config().
start_link(ClientId, Hosts, Config) ->
  ?LOG(info, "start link...~n ClientId: ~p~n Hosts: ~p~n Config: ~p", [ClientId, Hosts, Config]),
  {ConnectCfg0, CustomCfg} = split_config(Config),
  ConnectCfg = ConnectCfg0#{client_id => ClientId},

  State = #state{
    client_id = ClientId,
    seed_hosts = Hosts,
    conn_config = ConnectCfg,
    custom_config = CustomCfg,
    payload_connections = #{},
    metadata_ts = #{},
    leaders = #{}
  },
  case maps:get(reg_name, Config, false) of
    false -> gen_server:start_link(wolff_client, State, []);
    Name -> gen_server:start_link({local, Name}, wolff_client, State, [])
  end.

-spec stop(Client) -> Reply when
  Client :: pid(),
  Reply :: term().
stop(Client) ->
  ?LOG(info, "stop...~n Client: ~p", [Client]),
  safe_gen_call(Client, stop, infinity).

-spec get_id(Client) -> Reply when
  Client :: wolff:client(),
  Reply :: term().
get_id(Client) ->
  ?LOG(info, "get_id...~n Client: ~p", [Client]),
  safe_gen_call(Client, get_id, infinity).

-spec get_leader_connections(Client, Topic) -> Reply when
  Client :: wolff:client(),
  Topic :: topic(),
  Reply :: term().
get_leader_connections(Client, Topic) ->
  ?LOG(info, "get_leader_connections...~n Client: ~p~n Topic: ~p", [Client, Topic]),
  safe_gen_call(Client, {get_leader_connections, Topic}, infinity).

-spec get_leader_connection(Client, Topic, Partition) -> Reply when
  Client :: wolff:client(),
  Topic :: topic(),
  Partition :: partition(),
  Reply :: term().
get_leader_connection(Client, Topic, Partition) ->
  ?LOG(info, "get_leader_connections...~n Client: ~p~n Topic: ~p~n Partition:~p~n", [Client, Topic, Partition]),
  safe_gen_call(Client, {get_leader_connection, Topic, Partition}, infinity).

-spec recv_leader_connection(Client, Topic, Partition, Pid) -> 'ok' when
  Client :: term(),
  Topic :: topic(),
  Partition :: partition(),
  Pid :: pid().
recv_leader_connection(Client, Topic, Partition, Pid) ->
  ?LOG(info, "recv_leader_connection...~n Client: ~p~n Topic: ~p~n Partition: ~p~n Pid: ~p", [Client, Topic, Partition, Pid]),
  gen_server:cast(Client, {recv_leader_connection, Topic, Partition, Pid}).

%% @doc Get broker endpoint and connection config for
%% connecting a group coordinator.
-spec get_group_coordinator(wolff:client(), wolff:group_id()) ->
  {ok, {wolff:endpoint(), wolff:conn_config()}} | {error, any()}.
get_group_coordinator(Client, GroupId) ->
  safe_gen_call(Client, {get_group_coordinator, GroupId}, infinity).

%% @doc Get number of partitions for a given topic.
-spec get_partitions_count(wolff:client(), topic()) -> {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) ->
  ?LOG(info, "get_partitions_count...~n Client:~p~n Topic:~p~n", [Client, Topic]),
  safe_gen_call(Client, {get_partitions_count, Topic}, infinity).

%% @doc Get consumer of the given topic-parition.
-spec get_consumer(wolff:client(), topic(), partition()) -> {ok, pid()} | {error, get_consumer_error()}.
get_consumer(Client, Topic, Partition) ->
  Key = ?CONSUMER_KEY(Topic, Partition),
  safe_gen_call(Client, {get_consumer, Key}, infinity).

%% @doc Register ConsumerPid. The pid is registered in an ETS
%% table, then the callers may lookup a consumer pid from the table ane make
%% subscribe calls to the process directly.
-spec register_consumer(wolff:client(), topic(), partition(), pid()) -> ok.
register_consumer(Client, Topic, Partition, ConsumerPid) ->
  Key = ?CONSUMER_KEY(Topic, Partition),
  gen_server:cast(get_pid(Client), {register, Key, ConsumerPid}).

%% @doc De-register the consumer for a partition. The partition consumer
%% entry is deleted from the ETS table to allow cleanup of purposefully
%% stopped consumers and allow later restart.
-spec deregister_consumer(client(), topic(), partition()) -> ok.
deregister_consumer(Client, Topic, Partition) ->
  Key = ?CONSUMER_KEY(Topic, Partition),
  gen_server:cast(get_pid(Client), {deregister, Key}).

%% 回调接口
init(#state{client_id = ClientId} = Params) ->
  ?LOG(info, "init...~n Params: ~p", [Params]),
  erlang:process_flag(trap_exit, true),

  Tab = ets:new(?ETS(ClientId), [named_table, protected, {read_concurrency, true}]),
  State = Params#state{workers_tab = Tab},

  self() ! init,
  {ok, State}.

handle_info(init, State0) ->
  ?LOG(info, "handle_info<<init>>..."),
  State = ensure_metadata_connection(State0),
  {noreply, State};
handle_info(_Info, State) ->
  ?LOG(info, "handle_info...~n _Info: ~p~n State: ~p", [_Info, State]),
  {noreply, State}.

handle_call(stop, From, #state{payload_connections = Connections, meta_conn = MetaConn} = State) ->
  ?LOG(info, "handle_call<<stop>>...~n State: ~p", [State]),
  ok = close_connections(Connections),
  ok = close_connection(MetaConn),
  gen_server:reply(From, ok),
  {stop, normal, State#state{payload_connections = #{}, meta_conn = ?undef, metadata_ts = #{}}};
handle_call(get_id, _From, #state{client_id = Id} = State) ->
  ?LOG(info, "handle_call<<get_id>>...~n State: ~p", [State]),
  {reply, Id, State};
handle_call({get_leader_connections, Topic}, _From, State) ->
  ?LOG(info, "handle_call<<get_leader_connections>>...~n Topic: ~p~n State: ~p", [Topic, State]),
  case ensure_leader_connections(State, Topic) of
    {ok, NewState} ->
      Result = do_get_leader_connections(NewState, Topic),
      ?LOG(info, "do get leader connections result: ~p", [Result]),
      {reply, {ok, Result}, NewState};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call({get_leader_connection, Topic, Partition}, _From, State) ->
  ?LOG(info, "handle_call<<get_leader_connection>>...~n Topic: ~p~n Partition:~p~n State: ~p", [Topic, Partition, State]),
  case ensure_leader_connection(State, Topic, Partition) of
    {ok, NewState} ->
      Result = do_get_leader_connection(NewState, Topic, Partition),
      {reply, Result, NewState};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call({get_group_coordinator, GroupId}, _From, State) ->
  ?LOG(info, "handle_call<<get_group_coordinator>>...~n GroupId:~p~n State:~p~n", [GroupId, State]),
  {Result, NewState} = do_get_group_coordinator(State, GroupId),
  {reply, Result, NewState};
handle_call({get_partitions_count, Topic}, _From, State) ->
  ?LOG(info, "handle_call<<get_partition_count>>...~n Topic:~p~n State:~p~n", [Topic, State]),
  {Result, NewState} = do_get_partitions_count(State, Topic),
  {reply, Result, NewState};
handle_call({get_consumer, Key}, _From, #state{client_id = ClientId, workers_tab = Tab} = State) ->
  ?LOG(info, "handle_call<<get_consumer>>...~n Key:~p~n State:~p~n", [Key, State]),
  Pid = case ets:lookup(Tab, Key) of
          [] ->
            %% 查找consumers_sup模块，找到其进程
            ?CONSUMER_KEY(Topic, Partition) = Key,
            {ok, ConsumerPid} = wolff_consumers_sup:find_consumer(ClientId, Topic, Partition),
            ConsumerPid;
          [?CONSUMER(_Topic, _Partition, ConsumerPid)] ->
            ConsumerPid
        end,
  {reply, {ok, Pid}, State};
handle_call(_Req, _From, State) -> {noreply, State}.

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
handle_cast({register, Key, Pid}, #state{workers_tab = Tab} = State) ->
  ?LOG(info, "register... Key:~p Pid:~p ets:~p~n", [Key, Pid, Tab]),
  Info = ets:info(Tab),
  ?LOG(info, "Tab info:~p", [Info]),
  ets:insert(Tab, {Key, Pid}),
  {noreply, State};
handle_cast({deregister, Key}, #state{workers_tab = Tab} = State) ->
  ?LOG(info, "deregister... Key:~p ets:~p~n", [Key, Tab]),
  ets:delete(Tab, Key),
  {noreply, State};
handle_cast(_Req, State) ->
  ?LOG(info, "handle_cast...~n _Req: ~p~n State: ~p", [_Req, State]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{payload_connections = Connections, meta_conn = MetaConn} = State) ->
  ?LOG(warning, "terminate...~n Reason:~p~n State: ~p", [Reason, State]),
  ok = close_connections(Connections),
  ok = close_connection(MetaConn),
  {ok, State#state{payload_connections = #{}, meta_conn = ?undef, metadata_ts = #{}}}.

%% 内部调用
split_config(Config) ->
  ConnCfgKeys = kpro_connection:all_cfg_keys(),
  Pred = fun({K, _V}) -> lists:member(K, ConnCfgKeys) end,
  {ConnectCfg, CustomCfg} = lists:partition(Pred, maps:to_list(Config)),
  {maps:from_list(ConnectCfg), maps:from_list(CustomCfg)}.

ensure_leader_connections(State, Topic) ->
  ?LOG(info, "ensure leader connections...~n State: ~p~n Topic: ~p", [State, Topic]),
  State1 = ensure_metadata_connection(State),
  case is_metadata_fresh(State1, Topic) of
    true -> {ok, State1};
    false -> do_ensure_leader_connections(State1, Topic)
  end.

ensure_leader_connection(State, Topic, Partition) ->
  State1 = ensure_metadata_connection(State),
  do_ensure_leader_connection(State1, Topic, Partition).

ensure_metadata_connection(#state{meta_conn = ?undef, seed_hosts = Hosts, conn_config = Config} = State) ->
  Pid = case kpro:connect_any(Hosts, Config) of
          {ok, PidX} -> PidX;
          {error, Reason} -> erlang:exit(Reason)
        end,
  State#state{meta_conn = Pid};
ensure_metadata_connection(State) ->
  State.

is_metadata_fresh(#state{metadata_ts = MetadataTs, custom_config = Config}, Topic) ->
  MinInterval = maps:get(min_metadata_refresh_interval, Config, 1000),
  case maps:get(Topic, MetadataTs, false) of
    false -> false;
    Ts -> timer:now_diff(erlang:timestamp(), Ts) < MinInterval * 1000
  end.

do_ensure_leader_connections(#state{} = State, Topic) ->
  ?LOG(info, "do_ensure_leader_connections...~n Topic:~p~n State:~p~n", [Topic, State]),
  case do_get_metadata(State, Topic) of
    {{ok, Metadata}, State1} ->
      case get_brokers_and_partition_meta(Metadata) of
        {ok, {Brokers, PartitionMeta}} ->
          NewState = lists:foldl(
            fun(P_Meta, StateIn) ->
              try
                make_leader_connection(StateIn, Brokers, Topic, P_Meta)
              catch
                error : Reason ->
                  ?LOG(info, "Bad metadata for ~p-~p, Reason=~p", [Topic, P_Meta, Reason]),
                  StateIn
              end
            end, State1#state{}, PartitionMeta),
          {ok, NewState};
        {error, Reason} ->
          {error, Reason}
      end;
    {error, _Reason} ->
      {error, failed_to_fetch_metadata}
  end.

do_ensure_leader_connection(#state{} = State, Topic, Partition) ->
  ?LOG(info, "do_ensure_leader_connection...~n Topic:~p~n State:~p~n", [Topic, State]),
  case do_get_metadata(State, Topic) of
    {{ok, Metadata}, State1} ->
      case get_brokers_and_partition_meta(Metadata) of
        {ok, {Brokers, PartitionMeta}} ->
          Metas = lists:filter(fun(P_Meta) -> Partition =:= kf(partition, P_Meta) end, PartitionMeta),
          case erlang:length(Metas) =:= 1 of
            true ->
              NewState = make_leader_connection(State1, Brokers, Topic, lists:last(Metas)),
              {ok, NewState};
            false ->
              {error, not_found_partition_meta}
          end;
        {error, Reason} ->
          {error, Reason}
      end;
    {error, _Reason} ->
      {error, failed_to_fetch_metadata}
  end.


do_get_metadata(#state{meta_conn = Conn, metadata = Metadata,
  metadata_ts = MetadataTs, custom_config = Config} = State, Topic) ->
  Topics = case Topic of
             all -> all; %% in case no topic is given, get all
             _ -> [Topic]
           end,
  Request = wolff_kafka_request:metadata(Conn, Topics),
  Timeout = timeout(Config),
  case kpro:request_sync(Conn, Request, Timeout) of
    {ok, #kpro_rsp{api = metadata, msg = Meta}} ->
      ?LOG(info, "api=metadata, Meta: ~p~n", [Meta]),
      NewState = State#state{metadata = Metadata#{Topic => Meta}, metadata_ts = MetadataTs#{Topic => erlang:timestamp()}},
      {{ok, Meta}, NewState};
    {error, Reason} ->
      erlang:exit(Reason)
  end.

make_leader_connection(#state{conn_config = Config, payload_connections = Connections} = State, Brokers, Topic, P_Meta) ->
  ?LOG(info, "make_leader_connection...~n Brokers: ~p~n Topic: ~p~n P_Meta: ~p~n State: ~p",
    [Brokers, Topic, P_Meta, State]),

  Leaders = wolff_utils:to_val(State#state.leaders, #{}),
  ErrorCode = kf(error_code, P_Meta),
  ?IS_ERROR(ErrorCode) andalso erlang:error(ErrorCode),

  Partition = kf(partition, P_Meta),
  LeaderBrokerId = kf(leader, P_Meta),
  {_, Host} = lists:keyfind(LeaderBrokerId, 1, Brokers),
  Strategy = get_connection_strategy(State),
  ConnId = case Strategy of
             per_partition -> {Topic, Partition};
             per_broker -> Host
           end,

  ConnectFun = fun(Host) -> kpro:connect(Host, Config) end,

  NewConnections = case get_connected(ConnId, Host, Connections) of
                     already_connected -> Connections;
                     {needs_reconnect, OldConn} ->
                       ok = close_connection(OldConn),
                       add_conn(ConnectFun(Host), ConnId, Connections);
                     false ->
                       add_conn(ConnectFun(Host), ConnId, Connections)
                   end,
  NewState = State#state{payload_connections = NewConnections},
  case Strategy of
    per_broker ->
      NewLeaders = Leaders#{{Topic, Partition} => maps:get(ConnId, NewConnections)},
      NewState#state{leaders = NewLeaders};
    _ -> NewState
  end.

get_connection_strategy(#state{custom_config = Config}) ->
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

do_get_leader_connections(#state{payload_connections = Connections} = State, Topic) ->
  ?LOG(info, "do get leader connections...~n State: ~p~n Topic: ~p", [State, Topic]),
  FindInMap =
    case get_connection_strategy(State) of
      per_partition -> Connections;
      per_broker -> State#state.leaders
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

do_get_leader_connection(#state{payload_connections = Connections} = State, Topic, Partition) ->
  ?LOG(info, "do_get_leader_connection...~n State: ~p~n Topic: ~p~n Partition:~p~n", [State, Topic, Partition]),
  FindInMap =
    case get_connection_strategy(State) of
      per_partition -> Connections;
      per_broker -> State#state.leaders
    end,

  case maps:find({Topic, Partition}, FindInMap) of
    {ok, Pid} ->
      case is_alive(Pid) of
        true -> {ok, Pid};
        false -> {error, conn_pid_down}
      end;
    error ->
      {error, not_found_conn_pid}
  end.


do_get_group_coordinator(#state{client_id = ClientId, conn_config = Config} = State, GroupId) ->
  State1 = ensure_metadata_connection(State),
  Timeout = timeout(State1),
  case kpro:discover_coordinator(State1#state.meta_conn, group, GroupId, Timeout) of
    {ok, Endpoint} ->
      NewConfig = Config#{client_id => ensure_binary(ClientId)},
      {{ok, {Endpoint, NewConfig}}, State};
    {error, Reason} ->
      {{error, Reason}, State}
  end.

do_get_partitions_count(#state{} = State, Topic) ->
  F =
    fun(#state{metadata = Metadata} = StateIn) ->
      Meta = maps:get(Topic, Metadata, #{}),
      [TopicMeta] = kf(topic_metadata, Meta),
      ErrorCode = kf(error_code, TopicMeta),
      Partitions = kf(partition_metadata, TopicMeta),
      ?IS_ERROR(ErrorCode) andalso erlang:error(ErrorCode),

      {{ok, erlang:length(Partitions)}, StateIn}
    end,
  State = ensure_metadata_connection(State),
  case is_metadata_fresh(State, Topic) of
    true -> F(State);
    false ->
      {{ok, _Meta}, NewState} = do_get_metadata(State, Topic),
      F(NewState)
  end.

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

timeout(#state{custom_config = Config}) ->
  timeout(Config);
timeout(Config) ->
  T = maps:get(get_metadata_timeout_seconds, Config, ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  timer:seconds(T).

ensure_binary(ClientId) when is_atom(ClientId) ->
  ensure_binary(atom_to_binary(ClientId, utf8));
ensure_binary(ClientId) when is_binary(ClientId) ->
  ClientId.

safe_gen_call(Server, Call, Timeout) ->
  try
    Pid = get_pid(Server),
    gen_server:call(Pid, Call, Timeout)
  catch
    exit  : {noproc, _} ->
      ?LOG(error, "safe_gen_call failed... client_down"),
      {error, client_down};
    _Type : _Reason ->
      ?LOG(error, "safe_gen_call failed...~n error:~p~n Reason:~p~n", [_Type, _Reason])
  end.

get_pid(Client) when is_binary(Client) ->
  {ok, Pid} = wolff_client_sup:find_client(Client),
  Pid;
get_pid(Client) -> Client.

get_brokers_and_partition_meta(Metadata) ->
  BrokersMeta = kf(brokers, Metadata),
  Brokers = [parse_broker_meta(M) || M <- BrokersMeta],

  [TopicMeta] = kf(topic_metadata, Metadata),
  ErrorCode = kf(error_code, TopicMeta),
  case ErrorCode =:= no_error of
    true ->
      PartitionMeta = kf(partition_metadata, TopicMeta),
      {ok, {Brokers, PartitionMeta}};
    false -> {error, ErrorCode}
  end.

parse_broker_meta(BrokerMeta) ->
  BrokerId = kf(node_id, BrokerMeta),
  Host = kf(host, BrokerMeta),
  Port = kf(port, BrokerMeta),
  {BrokerId, {Host, Port}}.