%%%-------------------------------------------------------------------
%%% @author zxb
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. 5月 2020 下午2:01
%%%-------------------------------------------------------------------

%% @private
%% Version ranges are cached per host and per connection pid in ets

-module(wolff_kafka_apis).
-author("zxb").

-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(ETS, ?MODULE).

-include("logger.hrl").

-logger_header("[wolff kafka apis]").

%% API
-export([default_version/1, pick_version/2, start_link/0, stop/0]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

-export_type([api/0, vsn/0]).

-record(state, {}).

-type vsn() :: kpro:vsn().
-type api() :: kpro:api().
-type conn() :: kpro:connection().

%% @doc Start process.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
  gen_server:call(?SERVER, stop, infinity).

%% @doc Get default supported version for the given API.
-spec default_version(api()) -> vsn().
default_version(API) ->
  {Min, _Max} = supported_versions(API),
  Min.

%% @doc Pick API version for the given API.
-spec pick_version(conn(), api()) -> vsn().
pick_version(Conn, API) ->
  do_pick_version(Conn, API, supported_versions(API)).


%%%_* gen_server callbacks =====================================================

init([]) ->
  ?ETS = ets:new(?ETS, [named_table, public]),
  {ok, #state{}}.

handle_info({'DOWN', _Mref, process, Conn, _Reason}, State) ->
  ?LOG(info, "handle_info<<DOWN>>...~n _Reason:~p~n State:~p~n", [_Reason, State]),
  _ = ets:delete(?ETS, Conn),
  {noreply, State};

handle_info(Info, State) ->
  ?LOG(info, "handle_info...~n Info:~p~n State:~p~n", [Info, State]),
  {noreply, State}.

handle_cast({monitor_connection, Conn}, State) ->
  erlang:monitor(process, Conn),
  {noreply, State};

handle_cast(Cast, State) ->
  ?LOG(info, "handle_cast...~n Cast:~p~n State:~p~n", [Cast, State]),
  {noreply, State}.

handle_call(stop, From, State) ->
  ?LOG(info, "handle_call<<stop>>...~n State:~p~n", [State]),
  gen_server:reply(From, ok),
  {stop, normal, State};

handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internals ================================================================

do_pick_version(_Conn, _API, {V, V}) -> V;
do_pick_version(Conn, API, {Min, Max} = MyRange) ->
  case lookup_vsn_range(Conn, API) of
    none ->
      %% no version received from kafka, use min
      Min;
    {K_Min, K_Max} = Range when K_Min > Max orelse K_Max < Min ->
      erlang:error({unsupported_vsn_range, API, MyRange, Range});
    {_, K_Max} ->
      min(K_Max, Max)
  end.

%% Lookup API from cache, return 'none' if not found.
lookup_vsn_range(Conn, API) ->
  case ets:lookup(?ETS, Conn) of
    [] ->
      case kpro:get_api_versions(Conn) of
        {ok, Versions} when is_map(Versions) ->
          ?LOG(info, "lookup_vsn_range...~n Conn:~p~n API:~p~n Versions:~p~n", [Conn, API, Versions]),
          %% public ets, insert it by caller
          ets:insert(?ETS, {Conn, Versions}),
          %% tell ?SERVER to monitor the connection
          %% so to delete it from cache when 'DOWN' is received
          ok = monitor_connection(Conn),
          maps:get(API, Versions, none);
        {error, _Reason} ->
          none % connection dead ignore
      end;
    [{Conn, Vsn}] ->
      maps:get(API, Vsn, none)
  end.

monitor_connection(Conn) ->
  gen_server:cast(?SERVER, {monitor_connection, Conn}).

%% Do not change range without verification.
supported_versions(API) ->
  case API of
    produce -> {0, 5};
    fetch -> {0, 7};
    list_offsets -> {0, 2};
    metadata -> {0, 2};
    offset_commit -> {2, 2};
    offset_fetch -> {1, 2};
    find_coordinator -> {0, 0};
    join_group -> {0, 0};
    heartbeat -> {0, 0};
    leave_group -> {0, 0};
    sync_group -> {0, 0};
    describe_groups -> {0, 0};
    list_groups -> {0, 0};
    create_topics -> {0, 0};
    delete_topics -> {0, 0};
    _ -> erlang:error({unsupported_api, API})
  end.
