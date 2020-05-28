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
%%% Implement `wolff_group_member' behaviour callbacks to allow a
%%% process to act as a group member without having to deal with Kafka
%%% group protocol details. A typical workflow:
%%%
%%% 1. Spawn a group coordinator by calling
%%%    {@link wolff_group_coordinator:start_link/6}.
%%%
%%% 2. Subscribe to partitions received in the assignments from
%%%    `assignments_received/4' callback.
%%%
%%% 3. Receive messages from the assigned partitions (delivered by
%%%    the partition workers (the pollers) implemented in `wolff_consumer').
%%%
%%% 4. Unsubscribe from all previously subscribed partitions when
%%%    `assignments_revoked/1' is called.
%%%
%%% For group members that commit offsets to Kafka, do:
%%%
%%% 1. Call {@link wolff_group_coordinator:ack/4}. to acknowledge sucessful
%%%    consumption of the messages. Group coordinator will commit the
%%%    acknowledged offsets at configured interval.
%%%
%%% 2. Call {@link wolff_group_coordinator:commit_offsets/2}
%%%    to force an immediate offset commit if necessary.
%%%
%%% For group members that manage offsets locally, do:
%%%
%%% 1. Implement the `get_committed_offsets/2' callback.
%%%    This callback is evaluated every time when new assignments are received.
%%% @end
%%%=============================================================================

-module(wolff_group_member).
-author("zxb").

-optional_callbacks([
assign_partitions/3,
user_data/1
]).
%% Call the callback module to initialize assignments.
%% NOTE: This function is called only when `offset_commit_policy' is
%%       `consumer_managed' in group config.
%%       see wolff_group_coordinator:start_link/6. for more group config details
%% NOTE: The committed offsets should be the offsets for successfully processed
%%       (acknowledged) messages, not the begin-offset to start fetching from.
-callback get_committed_offsets(pid(), [{wolff:topic(), wolff:partition()}]) ->
  {ok, [{{wolff:topic(), wolff:partition()}, wolff:offset()}]}.

%% Called when the member is elected as the consumer group leader.
%% The first element in the group member list is ensured to be the leader.
%% NOTE: this function is called only when 'partition_assignment_strategy' is
%% 'callback_implemented' in group config.
%% see wolff_group_coordinator:start_link/6. for more group config details.
-callback assign_partitions(pid(), [wolff:group_member()], [{wolff:topic(), wolff:partition()}]) ->
  [{wolff:group_member_id(), [wolff:partition_assignment()]}].

%% Called when assignments are received from group leader.
%% the member process should now call wolff:subscribe/5
%% to start receiving message from kafka.
-callback assignments_received(pid(), wolff:group_member_id(),
    wolff:group_generation_id(), wolff:received_assignments()) ->
  ok.

%% Called before group re-balancing, the member should call
%% wolff:unsubscribe/3 to unsubscribe from all currently subscribed partitions.
-callback assignments_revoked(pid()) ->
  ok.

%% Called when making join request. This metadata is to let group leader know
%% more details about the member. e.g. its location and or capacity etc.
%% so that leader can make smarter decisions when assigning partitions to it.
-callback user_data(pid()) -> binary().