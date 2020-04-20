%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. 4月 2020 下午1:54
%%%-------------------------------------------------------------------
-module(wolff_stats).
-author("zxb").

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% API
-export([start_link/0, recv/4, sent/4, get_stats/0, get_stats/3]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, code_change/3, terminate/2]).

-logger_header("[wolff stats]").

%% export fun
start_link() ->
  ?LOG(warning, "start link..."),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

recv(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ?LOG(warning, "recv... ClientId: ~p, Topic: ~p, Partition: ~p, Numbers: ~p", [ClientId, Topic, Partition, Numbers]),
  ok = bump_counter({recv_cnt, ClientId, Topic, Partition}, Cnt),
  ok = bump_counter({recv_oct, ClientId, Topic, Partition}, Oct),
  gen_server:cast(wolff_stats, {recv, Numbers}).

sent(ClientId, Topic, Partition, #{cnt := Cnt, oct := Oct} = Numbers) ->
  ?LOG(warning, "sent... ClientId: ~p, Topic: ~p, Partition: ~p, Numbers: ~p", [ClientId, Topic, Partition, Numbers]),
  ok = bump_counter({send_cnt, ClientId, Topic, Partition}, Cnt),
  ok = bump_counter({send_oct, ClientId, Topic, Partition}, Oct),
  gen_server:cast(wolff_stats, {sent, Numbers}).

get_stats() ->
  ?LOG(warning, "get stats..."),
  gen_server:call(wolff_stats, get_stats, infinity).

get_stats(ClientId, Topic, Partition) ->
  ?LOG(warning, "get stats... ClientId: ~p, Topic: ~p, Partition: ~p", [ClientId, Topic, Partition]),
  #{
    send_cnt => get_counter({send_cnt, ClientId, Topic, Partition}),
    send_oct => get_counter({send_oct, ClientId, Topic, Partition}),
    recv_cnt => get_counter({recv_cnt, ClientId, Topic, Partition}),
    recv_oct => get_counter({recv_oct, ClientId, Topic, Partition})
  }.

%% gen_server callback fun
init([]) ->
  ?LOG(warning, "init..."),
  {ok, #{
    ets => ets:new(wolff_stats, [
      named_table, public, {write_concurrency, true}]),
    send_cnt => 0,
    send_oct => 0,
    recv_cnt => 0,
    recv_oct => 0
  }}.

handle_cast({recv, Numbers}, #{recv_oct := TotalOct, recv_cnt := TotalCnt} = State) ->
  ?LOG(warning, "handle case recv... State: ~p", [State]),
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply, State#{recv_oct := TotalOct + Oct, recv_cnt := TotalCnt + Cnt}};
handle_cast({sent, Numbers}, #{sent_oct := TotalOct, sent_cnt := TotalCnt} = State) ->
  ?LOG(warning, "handle case sent... State: ~p", [State]),
  #{cnt := Cnt, oct := Oct} = Numbers,
  {noreply, State#{sent_oct := TotalOct + Oct, sent_cnt := TotalCnt + Cnt}};
handle_cast(_Cast, _State) ->
  ?LOG(warning, "handle case... _Cast: ~p, _State: ~p", [_Cast, _State]),
  {noreply, _State}.

handle_call(get_stats, _From, State) ->
  ?LOG(warning, "handle call get_stats... State: ~p", [State]),
  Result = maps:with([send_cnt, send_oct, recv_cnt, recv_oct], State),
  {reply, Result, State};
handle_call(_Call, _Form, State) ->
  ?LOG(warning, "handle call... _Call: ~p,  State: ~p", [_Call, State]),
  {noreply, State}.

handle_info(_Info, State) ->
  ?LOG(warning, "handle_info: ~p, State: ~p", [_Info, State]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  ?LOG(warning, "code_change! OldVsn: ~p, State: ~p, _Extra: ~p", [_OldVsn, State, _Extra]),
  {ok, State}.

terminate(_Reason, _State) ->
  ?LOG(warning, "terminate! _Reason: ~p", [_Reason]),
  ok.

%% internal fun
bump_counter(Key, Inc) ->
  try
    _ = ets:update_counter(wolff_stats, Key, Inc, {Key, 0}),
    ok
  catch
    _:_ -> ok
  end.

get_counter(Key) ->
  case ets:lookup(wolff_stats, Key) of
    [] -> 0;
    [{_, Value}] -> Value
  end.