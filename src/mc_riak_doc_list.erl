%%%-------------------------------------------------------------------
%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%% @doc
%%%
%%% @end
%%% Created : 22 Apr 2010 by Daniel Reverri <dan@appush.com>
%%%-------------------------------------------------------------------
-module(mc_riak_doc_list).

-behaviour(gen_server).

%% API
-export([new/1,
         open/1,
         open/2,
         add/3,
         remove/3,
         list/1,
         save/1,
         save/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(BUCKET, <<"mc_riak_doc_list">>).

-record(state, {object, list = [], metadata = dict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create a new list
%%
%% @spec new(Name) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
new(Name) ->
  O = riakc_obj:new(?BUCKET, Name),
  gen_server:start_link(?MODULE, [new, O], []).

open(Name) ->
  open(Name, []).

open(Name, Options) ->
  case mc_riak_client:get(?BUCKET, Name, Options) of
    {ok, O} ->
      gen_server:start_link(?MODULE, [open, O], []);
    Other -> Other
  end.

add(Pid, Bucket, Key) ->
  gen_server:call(Pid, {add, {Bucket, Key}}).

remove(Pid, Bucket, Key) ->
  gen_server:call(Pid, {remove, {Bucket, Key}}).

list(Pid) ->
  gen_server:call(Pid, list).

save(Pid) ->
  save(Pid, []).

save(Pid, Options) ->
  gen_server:call(Pid, {save, Options}).

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
init([new, O]) ->
  {ok, #state{object=O}};

init([open, O]) ->
  {M, L} = read_content(O),
  {ok, #state{list=L, metadata=M, object=O}}.

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
handle_call({Action, Elem}, _From, State=#state{list=List}) when Action =:= add; Action =:= remove ->
  List1 = [{now(), Action, Elem}|List],
  {reply, ok, State#state{list=List1}};

handle_call(list, _From, State=#state{list=List}) ->
  List1 = extract_elements(merge_list(List)),
  {reply, List1, State};

handle_call({save, Options}, _From, State=#state{list=L, object=O, metadata=M}) ->
  O1 = write_content(O, M, L),
  {ok, O2} = mc_riak_client:put(O1, [return_body|Options]),
  {M1, L1} = read_content(O2),
  {reply, ok, State#state{list=L1, object=O2, metadata=M1}};

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

%% @doc
%% A changelog of elements is maintained per list. The most recent change (add|remove)
%% per element is tracked. For example:
%% After adding two elements (1,2):
%% [{time(), add, 2}, {time(), add, 1}]
%% After removing element 1:
%% [{time(), remove, 1}, {time(), add, 2}]
%%
%% When merging sibling lists (e.g. after a netsplit) we can use the timestamp
%% to ensure removals are merged correctly. For example, consider the following
%% Sibling 1:
%% [{time(), add, 3}, {time(), add, 2}, {time(), add, 1}]
%% Sibling 2:
%% [{time(), remove, 1}, {time(), add, 2}]
%% Merging would result in:
%% [{time(), add, 3}, {time(), remove, 1}, {time(), add, 2}]
%%
%% If removals were not tracked, the removal of 1 would be lost in the merge.
%% TODO: Prune old removes to prevent changelog from growing too large
merge_list(List) ->
  %% sort by timestamp
  L1 = lists:keysort(1, List),
  %% reverse list (most recent at head of list)
  L2 = lists:reverse(L1),
  %% remove duplicate elements (keep only the most recent entry per element)
  lists:ukeysort(3, L2).

extract_elements(List) ->
  [Elem ||{_, add, Elem} <-List].

make_links(List) ->
  [{BK,<<"mc_riak_doc_list">>} || BK <- List].

write_content(Object, Metadata, List) ->
  L1 = merge_list(List),
  L2 = extract_elements(L1),
  M1 = dict:store(<<"Links">>, make_links(L2), Metadata),
  V = bert:encode(L1),
  O1 = riakc_obj:update_value(Object, V, <<"application/bert">>),
  riakc_obj:update_metadata(O1, M1).

read_content(Object) ->
  %% Sort contents by last modified
  Contents = mc_riak_doc_rw:sort_contents(riakc_obj:get_contents(Object)),
  %% Grab most recent metadata
  [{M,_}|_] = Contents,
  %% Decode and merge values
  L = lists:foldl(fun({_,V},Acc) ->
                  lists:merge(bert:decode(V),Acc)
              end,
              [],
              Contents),
  L1 = merge_list(L),
  {M, L1}.

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

mc_riak_doc_list_test_() ->
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
                     mc_riak_doc_list_tests()
             end
     end}}.

mc_riak_doc_list_tests() ->
    [
     {"new should return {ok, Pid}",
      ?_test(
         begin
           {ok, _} = ?MODULE:new(<<"listname">>)
         end)},

     {"add should return ok",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"listname">>),
           ok = ?MODULE:add(Pid, <<"bucket">>, <<"key">>)
         end)},

     {"remove should return ok",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"listname">>),
           ok = ?MODULE:remove(Pid, <<"bucket">>, <<"key">>)
         end)},

     {"list should return [Elements]",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"listname">>),
           ok = ?MODULE:add(Pid, <<"bucket">>, <<"key1">>),
           ok = ?MODULE:add(Pid, <<"bucket">>, <<"key2">>),
           L = ?MODULE:list(Pid),
           ?assert(lists:member({<<"bucket">>,<<"key1">>}, L)),
           ?assert(lists:member({<<"bucket">>,<<"key2">>}, L))
         end)},

     {"save should return ok",
      ?_test(
         begin
           {ok, Pid} = ?MODULE:new(<<"listname">>),
           ok = ?MODULE:save(Pid)
         end)},

     {"open should fetch a saved list",
      ?_test(
         begin
           reset_riak(),
           {ok, Pid} = ?MODULE:new(<<"listname">>),
           ok = ?MODULE:save(Pid),
           {ok, Pid1} = ?MODULE:open(<<"listname">>),
           [] = ?MODULE:list(Pid1)
         end)},

     {"open should return {error, notfound} for non-existing documents",
      ?_test(
         begin
           reset_riak(),
           {error, notfound} = ?MODULE:open(<<"notfound">>)
         end)}
     ].

-endif.
