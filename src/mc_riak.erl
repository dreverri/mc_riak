%%%-------------------------------------------------------------------
%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%% @doc
%%%
%%% @end
%%% Created : 25 Apr 2010 by Daniel Reverri <dan@appush.com>
%%%-------------------------------------------------------------------
-module(mc_riak).

-export([start/0, start_link/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
	ok ->
	    ok;
	{error, {already_started, App}} ->
	    ok
    end.

%% @spec start_link() -> {ok,Pid::pid()}
%% @doc Starts the app for inclusion in a supervisor tree
start_link() ->
  ensure_started(sasl),
  ensure_started(ibrowse),
  appush_api_sup:start_link().

%% @spec start() -> ok
%% @doc Start the appush_api server.
start() ->
  ensure_started(sasl),
  ensure_started(ibrowse),
  application:start(mc_riak).

%% @spec stop() -> ok
%% @doc Stop the appush_api server.
stop() ->
  Res = application:stop(mc_riak),
  application:stop(ibrowse),
  application:stop(sasl),
  Res.
