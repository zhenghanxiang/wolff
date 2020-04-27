%%%-------------------------------------------------------------------
%% @doc wolff public API
%% @end
%%%-------------------------------------------------------------------

-module(wolff_app).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-logger_header("[wolff app]").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  ?LOG(warning, "start..."),
  wolff_sup:start_link().

stop(_State) ->
  ?LOG(warning, "stop..."),
  ok.
