%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. 5月 2020 上午11:19
%%%-------------------------------------------------------------------

%% @doc Helper functions for building request messages.
-module(wolff_kafka_request).
-author("zxb").

-include("wolff.hrl").

%% API
-export([
  offset_commit/2,
  metadata/2,
  join_group/2,
  sync_group/2,
  offset_fetch/3,
  list_offsets/4,
  fetch/7
]).

-type topic() :: wolff:topic().
-type partition() :: wolff:partition().
-type offset() :: wolff:offset().
-type conn() :: kpro:connection().
-type vsn() :: wolff_kafka_apis:vsn().

%% @doc Make a `offset_commit' request.
-spec offset_commit(conn(), kpro:struct()) -> kpro:req().
offset_commit(Conn, Fields) ->
  make_req(offset_commit, Conn, Fields).

%% @doc Make a metadata request.
-spec metadata(vsn() | conn(), all | [topic()]) -> kpro:req().
metadata(Conn, Topics) when is_pid(Conn) ->
  Vsn = wolff_kafka_apis:pick_version(Conn, metadata),
  metadata(Vsn, Topics);
metadata(Vsn, Topics) ->
  kpro_req_lib:metadata(Vsn, Topics).

%% @doc Make a `join_group' request.
-spec join_group(conn(), kpro:struct()) -> kpro:req().
join_group(Conn, Fields) ->
  make_req(join_group, Conn, Fields).

%% @doc Make a `sync_group' request.
-spec sync_group(conn(), kpro:struct()) -> kpro:req().
sync_group(Conn, Fields) ->
  make_req(sync_group, Conn, Fields).

%% @doc Make a offset fetch request.
%% NOTE: empty topics list only works for kafka 0.10.2.0 or later
-spec offset_fetch(conn(), wolff:group_id(), Topics) -> kpro:req()
  when Topics :: [{topic(), [partition()]}].
offset_fetch(Conn, GroupId, Topics0) ->
  Topics = lists:map(
    fun({Topic, Partitions}) ->
      [
        {topic, Topic},
        {partitions, [[{partition, P}] || P <- Partitions]}
      ]
    end, Topics0
  ),
  Body = [
    {group_id, GroupId},
    {topics,
      case Topics of
        [] -> ?kpro_null;
        _ -> Topics
      end}
  ],
  Vsn = pick_version(offset_fetch, Conn),
  kpro:make_request(offset_fetch, Vsn, Body).

%% @doc Make a `list_offsets' request message for offset resolution.
%% In kafka protocol, -2 and -1 are semantic 'time' to request for
%% 'earliest' and 'latest' offsets.
%% In wolff implementation, -2, -1, 'earliest' and 'latest'
%% are semantic 'offset', this is why often a variable named
%% Offset is used as the Time argument.
-spec list_offsets(conn(), topic(), partition(), wolff:offset_time()) -> kpro:req().
list_offsets(ConnPid, Topic, Partition, TimeOrSemanticOffset) ->
  Time = ensure_integer_offset_time(TimeOrSemanticOffset),
  Vsn = pick_version(list_offsets, ConnPid),
  kpro_req_lib:list_offsets(Vsn, Topic, Partition, Time).

%% @doc Make a fetch request, If the first arg is a connection pid, call
%% `wolff_kafka_apis:pick_version/2' to resolve version.
-spec fetch(conn(), topic(), partition(), offset(),
    kpro:wait(), kpro:count(), kpro:count()) -> kpro:req().
fetch(Conn, Topic, Partition, Offset, WaitTime, MinBytes, MaxBytes) ->
  Vsn = pick_version(fetch, Conn),
  kpro_req_lib:fetch(Vsn, Topic, Partition, Offset,
    #{
      max_wait_time => WaitTime,
      min_bytes => MinBytes,
      max_bytes => MaxBytes
    }).

%%%_* Internal Functions =======================================================

make_req(API, Conn, Fields) when is_pid(Conn) ->
  Vsn = pick_version(API, Conn),
  make_req(API, Vsn, Fields);
make_req(API, Vsn, Fields) ->
  kpro:make_request(API, Vsn, Fields).

pick_version(_API, Vsn) when is_integer(Vsn) -> Vsn;
pick_version(API, Conn) when is_pid(Conn) -> wolff_kafka_apis:pick_version(Conn, API);
pick_version(API, _) -> wolff_kafka_apis:default_version(API).

-spec ensure_integer_offset_time(wolff:offset_time()) -> integer().
ensure_integer_offset_time(?OFFSET_EARLIEST) -> -2;
ensure_integer_offset_time(?OFFSET_LATEST) -> -1;
ensure_integer_offset_time(T) when is_integer(T) -> T.