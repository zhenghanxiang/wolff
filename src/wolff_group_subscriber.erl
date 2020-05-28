%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. 5月 2020 上午10:17
%%%-------------------------------------------------------------------

%%%=============================================================================
%%% @doc
%%% A group subscriber is a gen_server which subscribes to partition consumers
%%% (poller) and calls the user-defined callback functions for message
%%% processing.
%%%
%%% An overview of what it does behind the scene:
%%% <ol>
%%% <li>Start a consumer group coordinator to manage the consumer group states,
%%%     see {@link wolff_group_coordinator:start_link/6}</li>
%%% <li>Start (if not already started) topic-consumers (pollers) and subscribe
%%%     to the partition workers when group assignment is received from the
%%      group leader, see {@link wolff:start_consumer/3}</li>
%%% <li>Call `CallbackModule:handle_message/4' when messages are received from
%%%     the partition consumers.</li>
%%% <li>Send acknowledged offsets to group coordinator which will be committed
%%%     to kafka periodically.</li>
%%% </ol>
%%% @end
%%%=============================================================================

-module(wolff_group_subscriber).
-author("zxb").

-behaviour(gen_server).
-behaviour(wolff_group_member).

-include("wolff.hrl").

-logger_header("[wolff group subscriber]").

%% API
-export([
  start_link/6,
  start_link/7,
  stop/1,
  ack/4,
  ack/5,
  commit/1,
  commit/4,
  user_data/1
]).

%% callbacks for wolff_group_coordinator
-export([
  assignments_revoked/1,
  assignments_received/4,
  assign_partitions/3,
  get_committed_offsets/2
]).

-export([
  init/1,
  handle_cast/2,
  handle_info/2,
  handle_call/3,
  code_change/3,
  terminate/2
]).

-type cb_state() :: term().
-type member_id() :: wolff:group_member_id().

%% Initialize the callback module s state.
-callback init(wolff:group_id(), term()) -> {ok, cb_state()}.

%% Handle a message. Return one of:
%%
%% {ok, NewCallbackState}:
%%   The subscriber has received the message for processing async-ly.
%%   It should call wolff_group_subscriber:ack/4 to acknowledge later.
%%
%% {ok, ack, NewCallbackState}
%%   The subscriber has completed processing the message.
%%
%% {ok, ack_no_commit, NewCallbackState}
%%   The subscriber has completed processing the message, but it
%%   is not ready to commit offset yet. It should call
%%   wolff_group_subscriber:commit/4 later.
%%
%% While this callback function is being evaluated, the fetch-ahead
%% partition-consumers are fetching more messages behind the scene
%% unless prefetch_count and prefetch_bytes are set to 0 in consumer config.
%%
-callback handle_message(wolff:topic(), wolff:partition(), wolff:message() | wolff:message_set(), cb_state()) ->
  {ok, cb_state()} | {ok, ack, cb_state()} | {ok, ack_no_commit, cb_state()}.

%% This callback is called only when subscriber is to commit offsets locally
%% instead of kafka.
%% Return {ok, Offsets, cb_state()} where Offsets can be [],
%% or only the ones that are found in e.g. local storage or database.
%% For the topic-partitions which have no committed offset found,
%% the consumer will take 'begin_offset' in consumer config as the start point
%% of data stream. If 'begin_offset' is not found in consumer config, the
%% default value -1 (latest) is used.
%
% commented out as it's an optional callback
%-callback get_committed_offsets(wolff:group_id(),
%                                [{wolff:topic(), wolff:partition()}],
%                                cb_state()) ->  {ok,
%                                                 [{{wolff:topic(),
%                                                    wolff:partition()},
%                                                   wolff:offset()}],
%                                                 cb_state()}.
%
%% This function is called only when 'partition_assignment_strategy' is
%% 'callback_implemented' in group config.
%% The first element in the group member list is ensured to be the group leader.
%
% commented out as it's an optional callback
%-callback assign_partitions([wolff:group_member()],
%                            [{wolff:topic(), wolff:partition()}],
%                            cb_state()) -> [{wolff:group_member_id(),
%                                             [wolff:partition_assignment()]}].

-define(DOWN(Reason), {down, wolff_utils:os_time_utc_str(), Reason}).

-record(consumer, {
  topic_partition :: {wolff:topic(), wolff:partition()},
  consumer_pid :: ?undef    %% initial state
  | pid()   %% normal state
  | {down, string(), any()}, %% consumer restarting
  consumer_mref :: ?undef | reference(),
  begin_offset :: ?undef | wolff:offset(),
  acked_offset :: ?undef | wolff:offset(),
  last_offset :: ?undef | wolff:offset()
}).

-type consumer() :: #consumer{}.

-type ack_ref() :: {wolff:topic(), wolff:partition(), wolff:offset()}.

-record(state, {
  client_id :: wolff:client_id(),
  client_mref :: reference(),
  groupId :: wolff:group_id(),
  memberId :: ?undef | member_id(),
  generationId :: ?undef | wolff:group_generation_id(),
  coordinator :: pid(),
  consumers = [] :: [consumer()],
  consumer_config :: wolff:consumer_config(),
  is_blocked = false :: boolean(),
  subscribe_tref :: ?undef | reference(),
  cb_module :: module(),
  cb_state :: cb_state(),
  message_type :: message | message_set
}).

-type state() :: #state{}.

%%% delay 2 seconds retry the failed subscription to partition consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_SUBSCRIBE_PARTITIONS, 'subscribe_partitions').

%%_* APIs =====================================================================

%% @equiv start_link(ClientId, GroupId, Topics, GroupConfig, message, CbModule, CbInitArgs)
-spec start_link(wolff:client_id(), wolff:group_id(), [wolff:topic()], wolff:group_config(),
    module(), term()) -> {ok, pid()} | {error, any()}.
start_link(ClientId, GroupId, Topics, GroupConfig, CbModule, CbInitArgs) ->
  start_link(ClientId, GroupId, Topics, GroupConfig, message, CbModule, CbInitArgs).

%% @doc Start (link) a group subscriber.
%%
%% `ClientId': Client ID of the wolff client.
%%
%% `GroupId': Consumer group ID which should be unique per kafka cluster
%%
%% `Topics': Predefined set of topic names to join the group.
%%
%%   NOTE: The group leader member will collect topics from all members and
%%         assign all collected topic-partitions to members in the group.
%%         i.e. members can join with arbitrary set of topics.
%%
%% `GroupConfig': For group coordinator config and Partition consumer config,
%%    see {@link wolff_group_coordinator:start_link/6},
%%    see {@link wolff_consumer:start_link/4}
%%
%% `MessageType':
%%   The type of message that is going to be handled by the callback
%%   module. Can be either `message' or `message_set'.
%%
%% `CbModule':
%%   Callback module which should have the callback functions
%%   implemented for message processing.
%%
%% `CbInitArgs':
%%   The term() that is going to be passed to `CbModule:init/1' when
%%   initializing the subscriber.
%% @end
-spec start_link(wolff:client(), wolff:group_id(), [wolff:topic()], wolff:group_config(),
    message | message_set, module(), term()) -> {ok, pid()} | {error, any()}.
start_link(ClientId, GroupId, Topics, GroupConfig, MessageType, CbModule, CbInitArgs) ->
  ?LOG(info, "start_link~n ClientId:~p~n GroupId:~p~n Topics:~p~n GroupConfig:~p~n MessageType:~p~n CbModule:~p~n CbInitArgs:~p~n", [ClientId, GroupId, Topics, GroupConfig, MessageType, CbModule, CbInitArgs]),
  Args = {ClientId, GroupId, Topics, GroupConfig, MessageType, CbModule, CbInitArgs},
  gen_server:start_link(?MODULE, Args, []).

%% @doc Stop group subscriber, wait for pid `DOWN' before return.
-spec stop(pid()) -> ok.
stop(Pid) ->
  ?LOG(info, "stop...~n Pid:~p~n", [Pid]),
  MonitorRef = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', MonitorRef, process, Pid, _Reason} ->
      ok
  end.

%% @doc Acknowledge and commit an offset.
%% The subscriber may ack a later (greater) offset which will be considered
%% as multi-acking the earlier (smaller) offsets. This also means that
%% disordered acks may overwrite offset commits and lead to unnecessary
%% message re-delivery in case of restart.
%% @end
-spec ack(pid(), wolff:topic(), wolff:partition(), wolff:offset()) -> ok.
ack(Pid, Topic, Partition, Offset) ->
  ack(Pid, Topic, Partition, Offset, true).

%% @doc Acknowledge an offset.
%% This call may or may not commit group subscriber offset depending on
%% the value of `Commit' argument
%% @end
-spec ack(pid(), wolff:topic(), wolff:partition(), wolff:offset(), boolean()) -> ok.
ack(Pid, Topic, Partition, Offset, Commit) ->
  ?LOG(info, "ack..~n Pid:~p~n Topic:~p~n Partition:~p~n Offset:~p~n Commit:~p~n", [Pid, Topic, Partition, Offset, Commit]),
  gen_server:cast(Pid, {ack, Topic, Partition, Offset, Commit}).

%% @doc Commit all acked offsets. NOTE: This is an async call.
-spec commit(pid()) -> ok.
commit(Pid) ->
  ?LOG(info, "commit...~n Pid:~p~n", [Pid]),
  gen_server:cast(Pid, commit_offset).

%% @doc Commit offset for a topic. This is an asynchronous call
-spec commit(pid(), wolff:topic(), wolff:partition(), wolff:offset()) -> ok.
commit(Pid, Topic, Partition, Offset) ->
  ?LOG(info, "commit...~n Pid:~p~n Topic:~p~n Partition:~p~n Offset:~p~n", [Pid, Topic, Partition, Offset]),
  gen_server:cast(Pid, {commit_offset, Topic, Partition, Offset}).

user_data(_Pid) -> <<>>.

%%%_* APIs for group coordinator ===============================================

%% @doc Called by group coordinator before re-joining the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  ?LOG(info, "assignments_revoked...~n Pid:~p~n", [Pid]),
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%% @doc Called by group coordinator when there is new assignment received.
-spec assignments_received(pid(), member_id(), wolff:group_generation_id(),
    wolff:received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  ?LOG(info, "assignments_received...~n Pid:~p~n MemberId:~p~n GenerationId:~p~n TopicAssignments:~p~n", [Pid, MemberId, GenerationId, TopicAssignments]),
  gen_server:cast(Pid, {new_assignments, MemberId, GenerationId, TopicAssignments}).

%% @doc This function is called only when `partition_assignment_strategy'
%% is set for `callback_implemented' in group config.
%% @end
-spec assign_partitions(pid(), [wolff:group_member()], [{wolff:topic(), wolff:partition()}])
      -> [{member_id(), [wolff:partition_assignment()]}].
assign_partitions(Pid, Members, TopicPartitions) ->
  ?LOG(info, "assign_partitions...~n Pid:~p~n Members:~p~n TopicPartitions:~p~n", [Pid, Members, TopicPartitions]),
  gen_server:call(Pid, {assign_partitions, Members, TopicPartitions}, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%%
%% NOTE: This function is called only when `offset_commit_policy' is set to
%%       `consumer_managed' in group config.
%%
%% NOTE: The committed offsets should be the offsets for successfully processed
%%       (acknowledged) messages, not the `begin_offset' to start fetching from.
%% @end
-spec get_committed_offsets(pid(), [{wolff:topic(), wolff:partition()}])
      -> {ok, [{{wolff:topic(), wolff:partition()}, wolff:offset()}]}.
get_committed_offsets(Pid, TopicPartitions) ->
  ?LOG(info, "get_committed_offsets...~n Pid:~p~n TopicPartitions:~p~n", [Pid, TopicPartitions]),
  gen_server:call(Pid, {get_committed_offsets, TopicPartitions}, infinity).


%%%_* gen_server callbacks =====================================================

init({ClientId, GroupId, Topics, GroupConfig, MessageType, CbModule, CbInitArgs}) ->
  ?LOG(info, "init..."),
  ok = wolff_utils:assert_client(ClientId),
  ok = wolff_utils:assert_group_id(GroupId),
  ok = wolff_utils:assert_topics(Topics),
  CoordinatorConfig = maps:get(coordinator, GroupConfig, #{}),
  ConsumerConfig = maps:get(consumer, GroupConfig, #{}),
  {ok, CbState} = CbModule:init(GroupId, CbInitArgs),
  Pid = self(),
  {ok, CoordinatorPid} = wolff_group_coordinator:start_link(ClientId, GroupId, Topics, CoordinatorConfig, ?MODULE, Pid),
  {ok, ClientPid} = wolff_client_sup:find_client(ClientId),
  ?LOG(info, "ready return... CurrentPid:~p CoordinatorPid:~p ClientPid:~p~n", [Pid, CoordinatorPid, ClientPid]),
  State = #state{
    client_id = ClientId,
    client_mref = erlang:monitor(process, ClientPid),  %% @doc 对client建立监督,当client退出时,接受相关消息并处理
    groupId = GroupId,
    coordinator = CoordinatorPid,
    consumer_config = ConsumerConfig,
    cb_module = CbModule,
    cb_state = CbState,
    message_type = MessageType
  },
  {ok, State}.

handle_info({_ConsumerPid, #kafka_message_set{} = MessageSet}, State) ->
  ?LOG(info, "handle_info...~n _ConsumerPid:~p~n MessageSet:~p~n State:~p~n", [_ConsumerPid, MessageSet, State]),

  {noreply, State};
handle_info({'DOWN', MonitorRef, process, _Pid, _Reason}, #state{client_mref = MonitorRef} = State) ->
  ?LOG(warning, "handle_info<<DOWN client>>...~n _Pid:~p~n _Reason:~p~n State:~p~n", [_Pid, _Reason, State]),
  %% restart, my supervisor should restart me
  {stop, client_down, State};

handle_info({'DOWN', _MonitorRef, process, Pid, Reason}, #state{consumers = Consumers} = State) ->
  ?LOG(warning, "handle_info<<DOWN consumers>>...~n Pid:~p~n Reason:~p~n State:~p~n", [Pid, Reason, State]),
  case get_consumer(Pid, Consumers) of
    #consumer{} = Consumer ->
      NewConsumer = Consumer#consumer{consumer_pid = ?DOWN(Reason), consumer_mref = ?undef},
      NewConsumers = put_consumer(NewConsumer, Consumers),
      NewState = State#state{consumers = NewConsumers},
      {noreply, NewState};
    false ->
      {noreply, State}
  end;

handle_info(?LO_CMD_SUBSCRIBE_PARTITIONS, State) ->
  ?LOG(info, "handle_info<<?LO_CMD_SUBSCRIBE_PARTITIONS>>...~n State:~p~n", [State]),
  NewState = case State#state.is_blocked of
               true -> State;
               false ->
                 {ok, #state{} = St} = subscribe_partitions(State),
                 St
             end,
  Timer = start_subscribe_timer(?undef, ?RESUBSCRIBE_DELAY),
  {noreply, NewState#state{subscribe_tref = Timer}};

handle_info(_Info, State) ->
  ?LOG(info, "handle_info...~n Info:~p~n State:~p~n", [_Info, State]),
  {noreply, State}.

handle_call(unsubscribe_all_partitions, _From, #state{consumers = Consumers} = State) ->
  ?LOG(warning, "handle_call<<unsubscribe_all_partitions>>...~n State:~p~n", [State]),
  lists:foreach(
    fun(#consumer{consumer_pid = Pid, consumer_mref = Mref}) ->
      case is_pid(Pid) of
        true ->
          %% 解除订阅
          _ = wolff:unsubscribed(Pid, self()),
          _ = erlang:demonitor(Mref, [flush]);
        false ->
          ok
      end
    end, Consumers
  ),
  {reply, ok, State#state{consumers = [], is_blocked = true}};

handle_call({assign_partitions, Members, TopicPartitions}, _From,
    #state{cb_module = CbModule, cb_state = CbState} = State) ->
  ?LOG(info, "handle_call<<assign_partitions>>...~n Members:~p~n TopicPartitions:~p~n State:~p~n", [Members, TopicPartitions, State]),
  case CbModule:assign_partitions(Members, TopicPartitions, CbState) of
    {NewState, Result} ->
      {reply, Result, State#state{cb_state = NewState}};
    %% Returning an updated cb_state is optional and clients that implemented
    %% brod prior to version 3.7.1 need this backwards compatibly case clause
    Result when is_list(Result) ->
      {reply, Result, State}
  end;

handle_call({get_committed_offsets, TopicPartitions}, _From, #state{groupId = GroupId,
  cb_module = CbModule, cb_state = CbState} = State) ->
  ?LOG(info, "handle_call<<get_committed_offsets>>...~n TopicPartitions:~p~n State:~p~n", [TopicPartitions, State]),
  case CbModule:get_committed_offsets(GroupId, TopicPartitions, CbState) of
    {ok, Result, NewCbState} ->
      NewState = State#state{cb_state = NewCbState},
      {reply, {ok, Result}, NewState};
    Unknown ->
      erlang:error(bad_return_value, {CbModule, get_committed_offsets, Unknown})
  end;

handle_call(Call, _From, State) ->
  ?LOG(info, "handle_call...~n Call:~p~n State:~p~n", [Call, State]),
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(stop, State) ->
  ?LOG(info, "handle_cast<<stop>>...~n State:~p~n", [State]),
  {stop, normal, State};
handle_cast({ack, Topic, Partition, Offset, Commit}, State) ->
  ?LOG(info, "handle_cast<<ack>>...~n Topic:~p~n Partition:~p~n Offset:~p~n Commit:~p~n State:~p~n", [Topic, Partition, Offset, Commit, State]),
  AckRef = {Topic, Partition, Offset},
  NewState = handle_ack(AckRef, State, Commit),
  {noreply, NewState};
handle_cast(commit_offset, State) ->
  ?LOG(info, "handle_cast<<commit_offset>>...~n State:~p~n", [State]),
  ok = wolff_group_coordinator:commit_offsets(State#state.coordinator),
  {noreply, State};
handle_cast({commit_offset, Topic, Partition, Offset},
    #state{generationId = GenerationId, coordinator = CoordinatorPid} = State) ->
  ?LOG(info, "handle_cast<<commit_offset>>...~n Topic:~p~n Partition:~p~n Offset:~p~n State:~p~n", [Topic, Partition, Offset, State]),
  do_commit_ack(CoordinatorPid, GenerationId, Topic, Partition, Offset),
  {noreply, State};
handle_cast({new_assignments, MemberId, GenerationId, TopicAssignments},
    #state{client_id = ClientId, consumer_config = ConsumerCfg, subscribe_tref = Timer} = State) ->
  ?LOG(info, "handle_cast<<new_assignments>>...~n MemberId:~p~n GenerationId:~p~n TopicAssignments:~p~n State:~p~n", [MemberId, GenerationId, TopicAssignments, State]),
  AllTopics = lists:map(
    fun(#wolff_received_assignment{topic = Topic}) ->
      Topic
    end, TopicAssignments),

  lists:foreach(
    fun(Topic) ->
      wolff:start_consumers(ClientId, Topic, ConsumerCfg)
    end, lists:usort(AllTopics)),

  Consumers = [
    #consumer{topic_partition = {Topic, Partition}, consumer_pid = ?undef,
      begin_offset = BeginOffset, acked_offset = ?undef} ||
    #wolff_received_assignment{topic = Topic, partition = Partition,
      begin_offset = BeginOffset} <- TopicAssignments
  ],

  NewState = State#state{
    consumers = Consumers,
    is_blocked = false,
    memberId = MemberId,
    generationId = GenerationId,
    subscribe_tref = start_subscribe_timer(Timer, 0)
  },
  {noreply, NewState};
handle_cast(_Cast, State) ->
  ?LOG(info, "handle_cast...~n _Cast:~p~n State:~p~n", [_Cast, State]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{}) -> ok.


%%%_* Internal Functions =======================================================

subscribe_partitions(#state{client_id = ClientId, consumers = Consumers0} = State) ->
  Consumers = lists:map(fun(C) -> subscribe_partition(ClientId, C) end, Consumers0),
  {ok, State#state{consumers = Consumers}}.

subscribe_partition(ClientId, #consumer{topic_partition = {Topic, Partition},
  consumer_pid = Pid, begin_offset = BeginOffset0, acked_offset = AckedOffset,
  last_offset = LastOffset} = Consumer) ->
  case wolff_utils:is_pid_alive(Pid) of
    true -> Consumer;
    false when AckedOffset =/= LastOffset andalso LastOffset =/= ?undef ->
      %% The last fetched offset is not yet acked,
      %% do not re-subscribe now to keep it simple and slow.
      %% Otherwise if we subscribe with {begin_offset, LastOffset + 1}
      %% we may exceed pre-fetch window size.
      Consumer;
    false ->
      %% fetch from the last acked offset + 1
      %% otherwise fetch from the assigned begin_offset
      BeginOffset = case AckedOffset of
                      ?undef -> BeginOffset0;
                      N when N >= 0 -> N + 1
                    end,
      Options = case BeginOffset =:= ?undef of
                  true -> #{}; %% fetch from 'begin_offset' in consumer config
                  false -> #{begin_offset => BeginOffset}
                end,

      case wolff:subscribe(ClientId, self(), Topic, Partition, Options) of
        {ok, ConsumerPid} ->
          Mref = erlang:monitor(process, ConsumerPid),
          Consumer#consumer{consumer_pid = ConsumerPid, consumer_mref = Mref};
        {error, Reason} ->
          Consumer#consumer{consumer_pid = ?DOWN(Reason), consumer_mref = ?undef}
      end
  end.

-spec handle_ack(ack_ref(), state(), boolean()) -> state().
handle_ack({Topic, Partition, Offset} = _AckRef, #state{generationId = GenerationId,
  consumers = Consumers, coordinator = CoordinatorPid} = State, Commit) ->
  case get_consumer({Topic, Partition}, Consumers) of
    #consumer{consumer_pid = ConsumerPid} = Consumer when Commit ->
      ok = consume_ack(ConsumerPid, Offset),
      ok = do_commit_ack(CoordinatorPid, GenerationId, Topic, Partition, Offset),
      NewConsumer = Consumer#consumer{acked_offset = Offset},
      NewConsumers = put_consumer(NewConsumer, Consumers),
      State#state{consumers = NewConsumers};
    #consumer{consumer_pid = ConsumerPid} ->
      ok = consume_ack(ConsumerPid, Offset),
      State;
    false ->
      %% Stale async-ack, discard.
      State
  end.

%% Tell consumer process to fetch more (if pre-fetch count/byte limit allows).
consume_ack(ConsumerPid, Offset) ->
  is_pid(ConsumerPid) andalso wolff:consume_ack(ConsumerPid, Offset),
  ok.

%% Send an async message to group coordinator for offset commit.
do_commit_ack(CoordinatorPid, GenerationId, Topic, Partition, Offset) ->
  ok = wolff_group_coordinator:ack(CoordinatorPid, GenerationId, Topic, Partition, Offset).

get_consumer(Pid, Consumers) when is_pid(Pid) ->
  lists:keyfind(Pid, #consumer.consumer_pid, Consumers);
get_consumer({_, _} = TP, Consumers) ->
  lists:keyfind(TP, #consumer.topic_partition, Consumers).

put_consumer(#consumer{topic_partition = TP} = C, Consumers) ->
  lists:keyreplace(TP, #consumer.topic_partition, Consumers, C).

-spec start_subscribe_timer(?undef | reference(), timeout()) -> reference().
start_subscribe_timer(?undef, Delay) ->
  erlang:send_after(Delay, self(), ?LO_CMD_SUBSCRIBE_PARTITIONS);
start_subscribe_timer(Ref, _Delay) when is_reference(Ref) ->
  %% The old timer is not expired, keep waiting
  %% A bit delay on subscribing to wolff_consumer is fine
  Ref.