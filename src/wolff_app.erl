%%%-------------------------------------------------------------------
%% @doc wolff public API
%% @end
%%%-------------------------------------------------------------------

-module(wolff_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-include("logger.hrl").

-logger_header("[wolff app]").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  ?LOG(info, "start..."),
  wolff_sup:start_link().

stop(_State) ->
  ?LOG(info, "stop..."),
  ok.
