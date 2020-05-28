-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").
-include_lib("kafka_protocol/include/kpro_error_codes.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-ifndef(WOLFF_HRL).
-define(WOLFF_HRL, true).

-define(conn_down(Reason), {down, Reason}).
-define(conn_error(Reason), {error, Reason}).
-define(leader_connection(Pid), {leader_connection, Pid}).
-define(UNKNOWN_OFFSET, -1).

%% Is kafka error code
-define(IS_ERROR(EC), ((EC) =/= ?no_error)).
-define(undef, undefined).

-define(WOLFF_CONSUMER_GROUP_PROTOCOL_VERSION, 0).
-define(OFFSET_EARLIEST, earliest).
-define(OFFSET_LATEST, latest).
-define(IS_SPECIAL_OFFSET(O), (O =:= ?OFFSET_EARLIEST orelse
  O =:= ?OFFSET_LATEST orelse
  O =:= -2 orelse
  O =:= -1)).

-record(kafka_message_set, {
  topic :: wolff:topic(),
  partition :: wolff:partition(),
  high_wm_offset :: integer(),  %% max offset of the partition
  messages :: [wolff:message()] %% exposed to wolff user
  | kpro:incomplete_batch()
}).

-record(kafka_group_member_metadata, {
  version :: non_neg_integer(),
  topics :: [wolff:topic()],
  user_data :: binary()
}).

-record(wolff_received_assignment, {
  topic :: wolff:topic(),
  partition :: wolff:partition(),
  begin_offset :: undefined | wolff:offset()
}).

-record(kafka_fetch_error, {
  topic :: wolff:topic(),
  partition :: wolff:partition(),
  error_code :: wolff:error_code(),
  error_desc = ""
}).

-endif.