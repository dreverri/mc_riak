%%%-------------------------------------------------------------------
%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%% @doc
%%%
%%% @end
%%% Created : 20 Apr 2010 by Daniel Reverri <dan@appush.com>
%%%-------------------------------------------------------------------
-module(mc_riak_obj).

-behaviour(gen_server).

%% API
-export([new/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {value = dict:new(), metadata = dict:new(), object}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec new() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
new(B, K) ->
  gen_server:start_link(?MODULE, [new, B, K], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([new, B, K]) ->
    O = riakc_obj:new(B, K),
    {ok, #state{object=O}};

init([]) ->
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
%% Object values are stored in a dict and converted to a proplist later
handle_call({set, Key, Value}, _From, State=#state{value=Value0}) ->
    Value1 = dict:store(Key, Value, Value0),
    {reply, ok, State#state{value=Value1}};

%% Update object contents
%% Send object to riak (return_body reads the write)
%% Merge the returned contents
%% Update State
handle_call(put, _From, State=#state{value=Value, metadata=Metadata, object=Object}) ->
  O1 = update_content(Object, Metadata, Value),
  O2 = mc_riak_client:put(O1, [return_body]),
  {M1, V1} = merge_contents(O2),
  {reply, ok, State#state{value=V1, metadata=M1, object=O2}};

handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% Encode the value to JSON
%% Update the object value (including content-type)
%% Update the metadata
update_content(Object, Metadata, Value) ->
  V1 = mochijson2:encode({struct, dict:to_list(Value)}),
  O1 = riakc_obj:update_value(Object, V1, <<"application/json">>),
  riakc_obj:update_metadata(O1, Metadata).

%% Return most recent content
%% TODO: Append Links headers of all siblings
merge_contents(Object) ->
  Contents = riakc_obj:get_contents(Object),
  hd(lists:sort(fun compare_content_dates/2, Contents)).

compare_content_dates({M1, _},{M2, _}) ->
    % true if M1 was modifed later than M2
    compare_dates(
      dict:fetch(<<"X-Riak-Last-Modified">>, M1),
      dict:fetch(<<"X-Riak-Last-Modified">>, M2)).

%% @spec compare_dates(string(), string()) -> boolean()
%% @doc Compare two RFC1123 date strings or two now() tuples (or one
%%      of each).  Return true if date A is later than date B.
compare_dates(A={_,_,_}, B={_,_,_}) ->
    %% assume 3-tuples are now() times
    A > B;
compare_dates(A, B) when is_list(A) ->
    %% assume lists are rfc1123 date strings
    compare_dates(rfc1123_to_now(A), B);
compare_dates(A, B) when is_list(B) ->
    compare_dates(A, rfc1123_to_now(B)).

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

rfc1123_to_now(String) when is_list(String) ->
    GSec = calendar:datetime_to_gregorian_seconds(
             httpd_util:convert_request_date(String)),
    ESec = GSec-?SEC_TO_EPOCH,
    Sec = ESec rem 1000000,
    MSec = ESec div 1000000,
    {MSec, Sec, 0}.
