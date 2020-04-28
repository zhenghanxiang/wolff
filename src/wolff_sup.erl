%%%-------------------------------------------------------------------
%% @doc wolff top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(wolff_sup).

-behaviour(supervisor).

-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff sup]").

-export([start_link/0]).

-export([init/1]).

%% supervisor:start_link is synchronous.
%% It does not return until all child processes have been started.
start_link() ->
  supervisor:start_link({local, wolff_sup}, wolff_sup, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
  ?LOG(info, "init..."),
  SupFlags = #{strategy => one_for_all,
    intensity => 10,
    period => 5},

  ChildSpecs = [stats_worker(), client_sup(), producers_sup()],
  {ok, {SupFlags, ChildSpecs}}.

stats_worker() ->
  ?LOG(info, "stats worker..."),
  #{
    id => wolff_stats,
    start => {wolff_stats, start_link, []},
    restart => permanent,
    shutdown => 2000,
    type => worker,
    modules => [wolff_stats]
  }.

client_sup() ->
  ?LOG(info, "client sup..."),
  #{
    id => wolff_client_sup,
    start => {wolff_client_sup, start_link, []},
    restart => permanent,
    shutdown => infinity,
    type => supervisor,
    modules => [wolff_client_sup]
  }.

producers_sup() ->
  ?LOG(info, "producers sup..."),
  #{
    id => wolff_producers_sup,
    start => {wolff_producers_sup, start_link, []},
    restart => permanent,
    shutdown => infinity,
    type => supervisor,
    modules => [wolff_producers_sup]
  }.