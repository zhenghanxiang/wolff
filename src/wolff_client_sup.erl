%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. 4月 2020 下午4:48
%%%-------------------------------------------------------------------
-module(wolff_client_sup).
-author("zxb").

-behaviour(supervisor).

-include_lib("emqx/include/logger.hrl").

%% API
-export([start_link/0, ensure_present/3, ensure_absence/1, find_client/1]).

-export([init/1]).

-logger_header("[wolff client sup]").

%% export fun
start_link() ->
  ?LOG(info, "start link..."),
  supervisor:start_link({local, wolff_client_sup}, wolff_client_sup, []).

-spec ensure_present(ClientId, Hosts, Config) -> Result when
  ClientId :: wolff:client_id(),
  Hosts :: [wolff:host()],
  Config :: wolff_client:config(),
  Result :: {'ok', pid()} | {'error', client_not_running}.
ensure_present(ClientId, Hosts, Config) ->
  ?LOG(info, "ensure present...~n ClientId: ~p~n Hosts: ~p~n Config: ~p", [ClientId, Hosts, Config]),
  ChildSpec = child_spec(ClientId, Hosts, Config),
  case supervisor:start_child(wolff_client_sup, ChildSpec) of
    {ok, Pid} ->
      ?LOG(info, "add child process success. Pid: ~p", [Pid]),
      {ok, Pid};
    {error, {already_started, Pid}} ->
      ?LOG(info, "pid already_started. Pid: ~p", [Pid]),
      {ok, Pid};
    {error, already_present} ->
      ?LOG(info, "error! pid already_present"),
      {error, client_not_running}
  end.

-spec ensure_absence(ClientId) -> ok when
  ClientId :: wolff:client_id().
ensure_absence(ClientId) ->
  ?LOG(info, "ensure absence... ClientId: ~p", [ClientId]),
  case supervisor:terminate_child(wolff_client_sup, ClientId) of
    ok -> supervisor:delete_child(wolff_client_sup, ClientId);
    {error, not_found} -> ok
  end.

-spec find_client(ClientId) -> Result when
  ClientId :: wolff:client_id(),
  Result :: {'ok', pid()} | {'error', any()}.
find_client(ClientId) ->
  ?LOG(info, "find client... ClientId: ~p", [ClientId]),
  Children = supervisor:which_children(wolff_client_sup),
  ?LOG(info, "Children: ~p", [Children]),
  case lists:keyfind(ClientId, 1, Children) of
    {ClientId, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    {ClientId, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.

%% supervisor callback fun
init([]) ->
  ?LOG(info, "init..."),
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  ChildSpecs = [],  %% 初始启动不指定子进程，以动态的方式添加
  {ok, {SupFlags, ChildSpecs}}.


%% internal func
child_spec(ClientId, Hosts, Config) ->
  #{
    id => ClientId,
    start => {wolff_client, start_link, [ClientId, Hosts, Config]},
    restart => transient,
    type => worker,
    modules => [wolff_client]
  }.