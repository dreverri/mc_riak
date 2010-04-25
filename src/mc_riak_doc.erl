%%%-------------------------------------------------------------------
%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% -------------------------------------------------------------------

%%% @doc
%%%
%%% @end
%%% Created : 20 Apr 2010 by Daniel Reverri <dan@appush.com>
%%%-------------------------------------------------------------------
-module(mc_riak_doc).

-behaviour(gen_server).

%% API
-export([new/2,
         open/2,
         open/3,
         read/1,
         set/3,
         set_from_list/2,
         get/2,
         to_list/1,
         to_json/1,
         save/1,
         save/2,
         delete/1,
         delete/2,
         add_link/2,
         add_link/4,
         remove_link/2,
         remove_link/4
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {doc = dict:new(), metadata = dict:new(), object, links = []}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Create a new document
new(B, K) ->
  O = riakc_obj:new(B, K),
  gen_server:start_link(?MODULE, [O], []).

%% Open an existing document
open(B, K) ->
  open(B, K, []).

open(B, K, Options) ->
  case mc_riak_client:get(B, K, Options) of
    {ok, O} ->
      gen_server:start_link(?MODULE, [O], []);
    Other -> Other
  end.

read(Pid) ->
  gen_server:call(Pid, read).

%% Set a key/value pair for the document. Key and value must be JSON encodable
set(Pid, Key, Value) ->
  gen_server:call(Pid, {set, Key, Value}).

set_from_list(Pid, List) ->
  gen_server:call(Pid, {set_from_list, List}).

%% Get the value of a key in the document
get(Pid, Key) ->
  gen_server:call(Pid, {get, Key}).

to_list(Pid) ->
  gen_server:call(Pid, to_list).
  
%% Return an object as JSON
to_json(Pid) ->
  gen_server:call(Pid, to_json).

%% Save the document
%% This function will read it's write and update the document to reflect any changes
save(Pid) ->
  ?MODULE:save(Pid, []).

save(Pid, Options) ->
  gen_server:call(Pid, {save, Options}).

%% Delete the document
%% gen_server will stop after a delete
delete(Pid) ->
  delete(Pid, []).

delete(Pid, Options) ->
  gen_server:call(Pid, {delete, Options}).

%% Add link
add_link(Pid, Bucket, Key, Tag) ->
  add_link(Pid, {{Bucket, Key},Tag}).

add_link(Pid, Link) ->
  gen_server:call(Pid, {add_link, Link}).

%% Remove link
remove_link(Pid, Bucket, Key, Tag) ->
  remove_link(Pid, {{Bucket,Key},Tag}).

remove_link(Pid, Link) ->
  gen_server:call(Pid, {remove_link, Link}).

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
init([O]) ->
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
handle_call(read, _From, State=#state{object=O}) ->
  {M, D} = read_content(O),
  L = get_links(M),
  {reply, ok, State#state{doc=D, metadata=M, links=L}};

handle_call({set, Key, Value}, _From, State=#state{doc=Doc0}) ->
    Doc1 = dict:store(Key, Value, Doc0),
    {reply, ok, State#state{doc=Doc1}};

handle_call({set_from_list, List}, _From, State=#state{doc=Doc0}) ->
  Doc1 = lists:foldl(fun({Key, Value}, AccIn) ->
                  dict:store(Key, Value, AccIn)
              end,
              Doc0, List),
  {reply, ok, State#state{doc=Doc1}};

handle_call({get, Key}, _From, State=#state{doc=Doc}) ->
  {reply, dict:find(Key, Doc), State};

handle_call(to_list, _From, State=#state{doc=Doc}) ->
  {reply, dict:to_list(Doc), State};
  
handle_call(to_json, _From, State=#state{doc=Doc}) ->
  Json = {struct, dict:to_list(Doc) },
  {reply, mochijson2:encode(Json), State};

%% Update object contents
%% Send object to riak (return_body reads the write)
%% Merge the returned contents
%% Update State
handle_call({save, Options}, _From, State=#state{doc=Doc, metadata=Metadata, object=Object, links=Links}) ->
  M0 = dict:store(<<"Links">>, Links, Metadata),
  O1 = write_content(Object, M0, Doc),
  {ok, O2} = mc_riak_client:put(O1, [return_body|Options]),
  {M1, D1} = read_content(O2),
  L1 = get_links(M1),
  {reply, ok, State#state{doc=D1, metadata=M1, object=O2, links=L1}};

handle_call({delete, Options}, _From, State=#state{object=Object}) ->
  Bucket = riakc_obj:bucket(Object),
  Key = riakc_obj:key(Object),
  {stop, normal, mc_riak_client:delete(Bucket, Key, Options), State};

handle_call({add_link, Link}, _From, State=#state{links=Links}) ->
  Links1 = lists:usort([Link|Links]),
  {reply, ok, State#state{links=Links1}};

handle_call({remove_link, Link}, _From, State=#state{links=Links}) ->
  Links1 = lists:delete(Link, Links),
  {reply, ok, State#state{links=Links1}};

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

%% TODO: Make read, write functions pluggable
read_content(Object) ->
  mc_riak_doc_rw:read_json(Object).

write_content(Object, Metadata, Doc) ->
  mc_riak_doc_rw:write_json(Object, Metadata, Doc).

get_links(Metadata) ->
  case dict:find(<<"Links">>, Metadata) of
    {ok, Value} -> Value;
    _ -> []
  end.

%% ====================================================================
%% unit tests
%% ====================================================================

%% Tests disabled until they can be prevented from running when included
%% as a dependency.
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_IP, {127,0,0,1}).
-define(TEST_PORT, 8087).
-define(TEST_RIAK_NODE, 'riak@127.0.0.1').
-define(TEST_EUNIT_NODE, 'eunit@127.0.0.1').
-define(TEST_COOKIE, 'riak').

reset_riak() ->
    ?assertEqual(ok, maybe_start_network()),
    %% Until there is a good way to empty the vnodes, require the
    %% test to run with ETS and kill the vnode sup to empty all the ETS tables
    ok = rpc:call(?TEST_RIAK_NODE, application, set_env, [riak_kv, storage_backend, riak_kv_ets_backend]),
    ok = supervisor:terminate_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup),
    {ok, _} = supervisor:restart_child({riak_kv_sup, ?TEST_RIAK_NODE}, riak_kv_vnode_sup).

maybe_start_network() ->
    %% Try to spin up net_kernel
    os:cmd("epmd -daemon"),
    case net_kernel:start([?TEST_EUNIT_NODE]) of
        {ok, _} ->
            erlang:set_cookie(?TEST_RIAK_NODE, ?TEST_COOKIE),
            ok;
        {error, {already_started, _}} ->
            ok;
        X ->
            X
    end.

mc_riak_doc_test_() ->
    {setup,
     fun() ->
         ok = maybe_start_network(),

         %% Start the mc_riak_client gen_server
         mc_riak_client:start_link()
     end,
     {generator,
     fun() ->
             case net_adm:ping(?TEST_RIAK_NODE) of
                 pang ->
                     []; %% {skipped, need_live_server};
                 pong ->
                     mc_riak_doc_tests()
             end
     end}}.

mc_riak_doc_tests() ->
    [
     {"new should return ok",
      ?_test(
         begin
           {ok, _} = ?MODULE:new(<<"bucket">>,<<"key">>)
         end)},

     {"set should return ok",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>)
         end)},

     {"save should return ok",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:save(Pid)
         end)},

     {"open should return {ok, Pid}",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:save(Pid),
           {ok, _} = ?MODULE:open(<<"bucket">>, <<"key">>)
         end)},

     {"get should fetch property values of current document",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           ok = ?MODULE:save(Pid),
           {ok, <<"value">>} = ?MODULE:get(Pid, <<"property">>)
         end)},

     {"get should fetch property values of opened and read document",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>, <<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           ok = ?MODULE:save(Pid),
           {ok, Pid1} = ?MODULE:open(<<"bucket">>, <<"key">>),
           ok = ?MODULE:read(Pid1),
           {ok, <<"value">>} = ?MODULE:get(Pid1, <<"property">>)
         end)},

     {"set_from_list should set several properties in one call",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set_from_list(Pid, [{<<"one">>, 1}, {<<"two">>, 2}]),
           {ok, 1} = ?MODULE:get(Pid, <<"one">>),
           {ok, 2} = ?MODULE:get(Pid, <<"two">>)
         end)},

     {"delete should remove a document",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:save(Pid),
           ok = ?MODULE:delete(Pid),
           {error, notfound} = ?MODULE:open(<<"bucket">>,<<"key">>)
         end)},

     {"add_link should add a link to the document",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:add_link(Pid, {{<<"bucket">>,<<"key">>},<<"tag">>}),
           ok = ?MODULE:save(Pid),
           {ok, O} = mc_riak_client:get(<<"bucket">>, <<"key">>, []),
           M = riakc_obj:get_metadata(O),
           [{{<<"bucket">>,<<"key">>},<<"tag">>}] = dict:fetch(<<"Links">>, M)
         end)},

     {"remove_link should remove a link from the document",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:add_link(Pid, {{<<"bucket">>,<<"key">>},<<"tag">>}),
           ok = ?MODULE:save(Pid),
           ok = ?MODULE:remove_link(Pid, {{<<"bucket">>,<<"key">>},<<"tag">>}),
           ok = ?MODULE:save(Pid),
           {ok, O} = mc_riak_client:get(<<"bucket">>, <<"key">>, []),
           M = riakc_obj:get_metadata(O),
           ?assertException(error, badarg, dict:fetch(<<"Links">>, M))
         end)},

     {"getting a property of an unread document should return an error",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           ok = ?MODULE:save(Pid),
           {ok, Pid1} = ?MODULE:open(<<"bucket">>,<<"key">>),
           ?assertEqual(error, ?MODULE:get(Pid1, <<"property">>))
         end)},

     {"saving an unread document should clear its contents",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           ok = ?MODULE:save(Pid),
           {ok, Pid1} = ?MODULE:open(<<"bucket">>,<<"key">>),
           ok = ?MODULE:save(Pid1),
           {ok, Pid2} = ?MODULE:open(<<"bucket">>,<<"key">>),
           ok = ?MODULE:read(Pid2),
           ?assertEqual(error, ?MODULE:get(Pid2, <<"property">>))
         end)},
     {"to_json should return valid JSON",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           ?assertEqual([{<<"property">>,<<"value">>}], ?MODULE:to_list(Pid))
         end)}, 
             
     {"to_list should return a proplist",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"bucket">>,<<"key">>),
           ok = ?MODULE:set(Pid, <<"property">>, <<"value">>),
           Json = mochijson2:encode({struct, [{<<"property">>,<<"value">>}]}),
           ?assertEqual(Json, ?MODULE:to_json(Pid))
         end)}
     ].

-endif.
