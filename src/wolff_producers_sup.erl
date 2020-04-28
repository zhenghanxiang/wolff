%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. 4月 2020 上午10:59
%%%-------------------------------------------------------------------
-module(wolff_producers_sup).
-author("zxb").

-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff producers sup]").

-behaviour(supervisor).

%% API
-export([start_link/0, ensure_absence/2, ensure_present/3]).

-export([init/1]).

%% 对外接口
start_link() ->
  ?LOG(info, "start link..."),
  supervisor:start_link({local, wolff_producers_sup}, wolff_producers_sup, []).

-spec ensure_present(ClientId, Topic, Config) -> Result when
  ClientId :: wolff:client_id(),
  Topic :: kpro:topic(),
  Config :: wolff_producer:config(),
  Result :: {ok, pid()} | {error, client_not_running}.
ensure_present(ClientId, Topic, Config) ->
  ?LOG(info, "ensure present... ClientId: ~p, Topic: ~p, Config: ~p", [ClientId, Topic, Config]),
  ChildSpec = child_spec(ClientId, Topic, Config),
  case supervisor:start_child(wolff_producers_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

-spec ensure_absence(ClientId, Name) -> ok when
  ClientId :: wolff:client_id(),
  Name :: wolff:name().
ensure_absence(ClientId, Name) ->
  ?LOG(info, "ensure absence... ClientId: ~p, Name: ~p", [ClientId, Name]),
  Id = {ClientId, Name},
  case supervisor:terminate_child(wolff_producers_sup, Id) of
    ok -> ok = supervisor:delete_child(wolff_producers_sup, Id);
    {error, not_found} -> ok
  end.

%% 回调接口
init([]) ->
  ?LOG(warning, "init..."),
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  ChildSpecs = [], %% 动态添加子进程
  {ok, {SupFlags, ChildSpecs}}.


%% 内部接口
child_spec(ClientId, Topic, Config) ->
  #{id => {ClientId, get_name(Config)},
    start => {wolff_producers, start_link, [ClientId, Topic, Config]},
    restart => transient,
    type => worker,
    modules => [wolff_producers]}.

get_name(Config) ->
  maps:get(name, Config, wolff_producers).