%%%-------------------------------------------------------------------
%% @doc wolff public API
%% @end
%%%-------------------------------------------------------------------

-module(wolff_app).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-logger_header("[Wolff App]").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  ?LOG(warning, "Start..."),
  wolff_sup:start_link().

stop(_State) ->
  ?LOG(warning, "Stop..."),
  ok.
