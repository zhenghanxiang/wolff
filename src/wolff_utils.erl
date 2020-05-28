%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. 5月 2020 上午10:16
%%%-------------------------------------------------------------------
-module(wolff_utils).
-author("zxb").

-include("wolff.hrl").

%% API
-export([
  is_pid_alive/1,
  ok_when/2,
  assert_client/1,
  assert_group_id/1,
  assert_topics/1,
  assert_topic/1,
  to_val/2,
  kf/2,
  kf/3,
  resolve_offset/4
]).

-export([
  os_time_utc_str/0,
  group_per_key/1,
  group_per_key/2,
  request_sync/2,
  request_sync/3,
  parse_rsp/1,
  get_stable_offset/1,
  flatten_batches/3,
  is_normal_reason/1,
  ensure_atom/1
]).

-type connection() :: kpro:connection().
-type topic() :: wolff:topic().
-type partition() :: wolff:partition().
-type group_id() :: wolff:group_id().
-type offset_time() :: wolff:offset_time().
-type offset() :: wolff:offset().

%% Raise an 'error' exception when first argument is not 'true'.
%% The second argument is used as error reason.
-spec ok_when(boolean(), any()) -> ok | no_return().
ok_when(true, _) -> ok;
ok_when(_, Reason) -> erlang:error(Reason).

is_pid_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

%% @doc Assert client_id is an atom().
-spec assert_client(wolff:client_id() | pid()) -> ok | no_return().
assert_client(Client) ->
  ok_when(is_atom(Client) orelse is_pid(Client) orelse is_binary(Client), {bad_client, Client}).

%% @doc Assert group_id is a binary().
-spec assert_group_id(group_id()) -> ok | no_return().
assert_group_id(GroupId) ->
  ok_when(is_binary(GroupId) andalso size(GroupId) > 0, {bad_group_id, GroupId}).

%% @doc Assert a list of topic names [binary()].
-spec assert_topics([topic()]) -> ok | no_return().
assert_topics(Topics) ->
  Pred = fun(Topic) -> ok =:= assert_topic(Topic) end,
  ok_when(is_list(Topics) andalso Topics =/= [] andalso lists:all(Pred, Topics),
    {bad_topics, Topics}).

%% @doc Assert topic is a binary().
-spec assert_topic(topic()) -> ok | no_return().
assert_topic(Topic) ->
  ok_when(is_binary(Topic) andalso size(Topic) > 0, {bad_topic, Topic}).

%% @doc Group values per-key in a key-value list.
-spec group_per_key([{Key, Value}]) -> [{Key, Value}] when
  Key :: term(),
  Value :: term().
group_per_key(List) ->
  lists:foldl(
    fun({Key, Value}, Acc) ->
      orddict:append_list(Key, [Value], Acc)
    end, [], List).

%% @doc Group values per-key for the map result of a list.
-spec group_per_key(fun((term()) -> {Key, Value}), [term()]) -> [{Key, Value}] when
  Key :: term(),
  Value :: term().
group_per_key(MapFun, List) ->
  group_per_key(lists:map(MapFun, List)).

%% @doc Get now timestamp, and format as UTC string.
-spec os_time_utc_str() -> string().
os_time_utc_str() ->
  Ts = os:timestamp(),
  {{Y, M, D}, {H, Min, Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

-spec request_sync(connection(), kpro:req()) -> ok | {ok, term()} | {error, any()}.
request_sync(Conn, Request) ->
  request_sync(Conn, Request, infinity).

-spec request_sync(connection(), kpro:req(), infinity | timeout()) -> ok | {ok, term()} | {error, any()}.
request_sync(Conn, #kpro_req{ref = Ref} = Request, Timeout) ->
  % kpro_connection has a global 'request_timeout' option
  % the connection pid will exit if that one times out
  case kpro:request_sync(Conn, Request, Timeout) of
    {ok, #kpro_rsp{ref = Ref} = Rsp} ->
      parse_rsp(Rsp);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Parse decoded kafka response (`#kpro_rsp{}') into a more generic
%% representation.
%% Return `ok' if it is a trivial 'ok or not' response without data fields
%% Return `{ok, Result}' for some of the APIs when no error-code found in
%% response. Result could be a transformed representation of response message
%% body `#kpro_rsp.msg' or the response body itself.
%% For some APIs, it returns `{error, CodeOrMessage}' when error-code is not
%% `no_error' in the message body.
%% NOTE: Not all error codes are interpreted as `{error, CodeOrMessage}' tuple.
%%       for some of the complex response bodies, error-codes are retained
%%       for caller to parse.
-spec parse_rsp(kpro:rsp()) -> ok | {ok, term()} | {error, any()}.
parse_rsp(#kpro_rsp{api = API, vsn = Vsn, msg = Msg}) ->
  try parse(API, Vsn, Msg) of
    ok -> ok;
    Result -> {ok, Result}
  catch
    throw : ErrorCodeOrMessage ->
      {error, ErrorCodeOrMessage}
  end.

to_val(Val, Default) ->
  case Val =:= ?undef of
    true -> Default;
    false -> Val
  end.

-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

-spec kf(kpro:field_name(), kpro:struct(), any()) -> kpro:field_value() | any().
kf(FieldName, Struct, Default) -> kpro:find(FieldName, Struct, Default).

%% @doc Resolve timestamp or semantic offset to real offset.
%% The give pid should be the connection to partition leader broker.
-spec resolve_offset(pid(), topic(), partition(), offset_time()) -> {ok, offset()} | {error, any()}.
resolve_offset(Pid, Topic, Partition, Time) ->
  Request = wolff_kafka_request:list_offsets(Pid, Topic, Partition, Time),
  case request_sync(Pid, Request) of
    {ok, #{error_code := EC}} when ?IS_ERROR(EC) ->
      {error, EC};
    {ok, #{offset := Offset}} ->
      {ok, Offset};
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc last_stable_offset is added in fetch response version 4
%% This function takes high watermark offset as last_stable_offset
%% in case it's missing.
get_stable_offset(Header) ->
  HighWmOffset = kf(high_watermark, Header),
  StableOffset = kf(last_stable_offset, Header, HighWmOffset),
  %% handle the case when high_watermark < last_stable_offset
  min(HighWmOffset, StableOffset).

%% @doc Make a flat message list from decoded batch list.
%% Return the next beging-offset together with the messages.
flatten_batches(BeginOffset, _, []) ->
  %% empty batch implies we have reached the end of a partition,
  %% we do not want to advance begin-offset here,
  %% instead, we should try again (after a delay) with the same offset
  {BeginOffset, []};
flatten_batches(BeginOffset, Header, Batches0) ->
  {LastMeta, _} = lists:last(Batches0),
  Batches = drop_aborted(Header, Batches0),
  MsgList = lists:append([Msgs || {Meta, Msgs} <- Batches, not is_control(Meta)]),

  case LastMeta of
    #{last_offset := LastOffset} ->
      %% For magic v2 messages, there is information about last
      %% offset of a given batch in its metadata.
      %% Make use of this information to fast-forward to the next
      %% batch's base offset.
      {LastOffset + 1, drop_old_messages(BeginOffset, MsgList)};
    _ when MsgList =/= [] ->
      #kafka_message{offset = Offset} = lists:last(MsgList),
      {Offset + 1, drop_old_messages(BeginOffset, MsgList)};
    _ ->
      %% Not much info about offsets, give it a try at the very next offset.
      {BeginOffset + 1, []}
  end.

%% @doc Check terminate reason for a gen_server implementation
is_normal_reason(normal) -> true;
is_normal_reason(shutdown) -> true;
is_normal_reason({shutdown, _}) -> true;
is_normal_reason(_) -> false.

ensure_atom(Binary) when is_binary(Binary) -> binary_to_atom(Binary, utf8);
ensure_atom(List) when is_list(List) -> list_to_atom(List);
ensure_atom(Atom) when is_atom(Atom) -> Atom.

%%%_* Internal functions =======================================================

parse(produce, _Vsn, Msg) ->
  kpro:find(base_offset, get_partition_rsp(Msg));
parse(fetch, _Vsn, Msg) ->
  parse_fetch_rsp(Msg);
parse(list_offsets, _, Msg) ->
  case get_partition_rsp(Msg) of
    #{offsets := []} = M -> M#{offset => -1};
    #{offsets := [Offset]} = M -> M#{offset => Offset};
    #{offset := _} = M -> M
  end;
parse(metadata, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_metadata, Msg)),
  Msg;
parse(find_coordinator, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(join_group, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(heartbeat, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(leave_group, _, Msg) ->
  ok = throw_error_code([Msg]);
parse(sync_group, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(describe_groups, _, Msg) ->
  %% return groups
  Groups = kpro:find(groups, Msg),
  ok = throw_error_code(Groups),
  Groups;
parse(list_groups, _, Msg) ->
  %% return groups
  ok = throw_error_code([Msg]),
  kpro:find(groups, Msg);
parse(create_topics, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_errors, Msg));
parse(delete_topics, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_error_codes, Msg));
parse(init_producer_id, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(create_partitions, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_errors, Msg));
parse(end_txn, _, Msg) ->
  ok = throw_error_code([Msg]);
parse(describe_acls, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(create_acls, _, Msg) ->
  ok = throw_error_code(kpro:find(creation_responses, Msg));
parse(_API, _Vsn, Msg) ->
  %% leave it to the caller to parse:
  %% offset_commit
  %% offset_fetch
  %% sasl_handshake
  %% api_versions
  %% delete_records
  %% add_partitions_to_txn
  %% txn_offset_commit
  %% delete_acls
  %% describe_acls
  %% describe_configs
  %% alter_configs
  %% alter_replica_log_dirs
  %% describe_log_dirs
  %% sasl_authenticate
  %% create_partitions
  %% create_delegation_token
  %% renew_delegation_token
  %% expire_delegation_token
  %% describe_delegation_token
  %% delete_groups
  Msg.

get_partition_rsp(Struct) ->
  [TopicRsp] = kpro:find(responses, Struct),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  PartitionRsp.

%% Parse fetch response into a more user-friendly representation.
parse_fetch_rsp(Msg) ->
  EC1 = kpro:find(error_code, Msg, ?no_error),
  SessionID = kpro:find(session_id, Msg, 0),
  {Header, Batches, EC2} = case kpro:find(responses, Msg) of
                             [] ->
                               %% a session init without data
                               {undefined, [], ?no_error};
                             _ ->
                               PartitionRsp = get_partition_rsp(Msg),
                               HeaderX = kpro:find(partition_header, PartitionRsp),
                               throw_error_code([HeaderX]),
                               Records = kpro:find(record_set, PartitionRsp),
                               ECx = kpro:find(error_code, HeaderX),
                               {HeaderX, kpro:decode_batches(Records), ECx}
                           end,
  ErrorCode = case EC2 =:= ?no_error of
                true -> EC1;
                false -> EC2
              end,

  case ?IS_ERROR(ErrorCode) of
    true -> erlang:throw(ErrorCode);
    false -> #{session_id => SessionID
      , header => Header
      , batches => Batches
    }
  end.

%% This function takes a list of kpro structs,
%% return ok if all structs have 'no_error' as error code.
%% Otherwise throw an exception with the first error.
throw_error_code([]) -> ok;
throw_error_code([Struct | Rest]) ->
  EC = kpro:find(error_code, Struct),
  case ?IS_ERROR(EC) of
    true ->
      Err = kpro:find(error_message, Struct, EC),
      erlang:throw(Err);
    false ->
      throw_error_code(Rest)
  end.

drop_aborted(#{aborted_transactions := AbortedL}, Batches) ->
  %% Drop batches for each abored transaction
  lists:foldl(
    fun(#{producer_id := ProducerId, first_offset := FirstOffset}, BatchesIn) ->
      do_drop_aborted(ProducerId, FirstOffset, BatchesIn, [])
    end, Batches, AbortedL);
drop_aborted(_, Batches) ->
  %% old version, no aborted_transactions field
  Batches.

do_drop_aborted(_, _, [], Acc) -> lists:reverse(Acc);
do_drop_aborted(ProducerId, FirstOffset, [{_Meta, []} | Batches], Acc) ->
  %% all messages are deleted (compacted topic)
  do_drop_aborted(ProducerId, FirstOffset, Batches, Acc);
do_drop_aborted(ProducerId, FirstOffset, [{Meta, Msgs} | Batches], Acc) ->
  #kafka_message{offset = BaseOffset} = hd(Msgs),
  case {is_txn(Meta, ProducerId), is_control(Meta)} of
    {true, true} ->
      %% this is the end of a transaction
      %% no need to scan remaining batches
      lists:reverse(Acc) ++ Batches;
    {true, false} when BaseOffset >= FirstOffset ->
      %% this batch is a part of aborted transaction, drop it
      do_drop_aborted(ProducerId, FirstOffset, Batches, Acc);
    _ ->
      do_drop_aborted(ProducerId, FirstOffset, Batches, [{Meta, Msgs} | Acc])
  end.

%% Return true if a batch is in a transaction from the given producer.
is_txn(#{is_transaction := true, producer_id := Id}, Id) -> true;
is_txn(_Meta, _ProducerId) -> false.

is_control(#{is_control := true}) -> true;
is_control(_) -> false.

%% A fetched batch may contain offsets earlier than the
%% requested begin-offset because the batch might be compressed on
%% kafka side. Here we drop the leading messages.
drop_old_messages(_BeginOffset, []) -> [];
drop_old_messages(BeginOffset, [Message | Rest] = All) ->
  case Message#kafka_message.offset < BeginOffset of
    true -> drop_old_messages(BeginOffset, Rest);
    false -> All
  end.