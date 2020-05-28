%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. 5月 2020 上午10:16
%%%-------------------------------------------------------------------
-module(wolff_consumers_sup).
-author("zxb").

-behaviour(supervisor).

-include("wolff.hrl").

-logger_header("[wolff consumers sup]").

-define(CONSUMERS(ClientId, Topic), {ClientId, Topic}).

%% API
-export([
  start_link/0,
  ensure_present/3,
  ensure_absence/2,
  find_consumer/3
]).

-export([
  init/1
]).

-type topic() :: kpro:topic().
-type config() :: wolff:config().

%%%_* APIs =====================================================================

%% @doc Start a root consumers supervisor.
-spec start_link() -> {ok, pid()}.
start_link() ->
  ?LOG(info, "start_link..."),
  supervisor:start_link({local, wolff_consumers_sup}, wolff_consumers_sup, []).

%% @doc ensure the consumers is start.
-spec ensure_present(wolff:client_id(), topic(), config()) -> {ok, pid()} | {error, any()}.
ensure_present(ClientId, Topic, ConsumerCfg) ->
  ?LOG(info, "ensure_present...~n ClientId:~p~n Topic:~p~n ConsumerCfg:~p~n", [ClientId, Topic, ConsumerCfg]),
  ChildSpec = child_spec(ClientId, Topic, ConsumerCfg),
  case supervisor:start_child(wolff_consumers_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

%% @doc ensure the consumers is stop.
ensure_absence(ClientId, Name) ->
  ?LOG(info, "ensure_absence...~n ClientId:~p~n Name:~p~n", [ClientId, Name]),
  Id = {ClientId, Name},
  case supervisor:terminate_child(wolff_consumers_sup, Id) of
    ok -> ok = supervisor:delete_child(wolff_consumers_sup, Id);
    {error, not_found} -> ok
  end.

%% @doc find consumer process pid
find_consumer(ClientId, Topic, Partition) ->
  ?LOG(info, "find_consumer...~n ClientId:~p~n Topic:~p~n Partition:~p~n", [ClientId, Topic, Partition]),
  Children = supervisor:which_children(wolff_consumers_sup),
  ID = ?CONSUMERS(ClientId, Topic),
  case lists:keyfind(ID, 1, Children) of
    {ID, Pid, _, _} when is_pid(Pid) ->
      wolff_consumers:find_consumer(Pid, Topic, Partition);
    {ID, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.

%%%_* Supervisor Callback APIs =================================================
init([]) ->
  ?LOG(info, "init..."),
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  ChildSpecs = [], %% 动态添加子进程
  {ok, {SupFlags, ChildSpecs}}.


%%%_* Internal Callback APIs =================================================
child_spec(ClientId, Topic, Config) ->
  #{id => ?CONSUMERS(ClientId, Topic),
    start => {wolff_consumers, start_link, [ClientId, Topic, Config]},
    restart => transient,
    type => worker,
    modules => [wolff_consumers]}.