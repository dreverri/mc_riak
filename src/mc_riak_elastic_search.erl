%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%% @doc
%%%
%%% @end
%%% Created : 25 Apr 2010 by Daniel Reverri <dan@appush.com>

-module(mc_riak_elastic_search).

-define(URL, "http://localhost:9200").

-export([create_index/2,
         delete_index/1,
         refresh_index/1,
         put_mapping/3,
         index/4,
         get/3,
         delete/3,
         search/3,
         search_dsl/3
        ]).

%% curl -XPUT 'http://localhost:9200/twitter/' -d '
%% index :
%%     number_of_shards : 3
%%     number_of_replicas : 2
%% '
create_index(Name, Config) ->
  ibrowse:send_req(?URL ++ "/" ++ Name ++ "/", [], put, Config).

%% curl -XDELETE 'http://localhost:9200/twitter/'
delete_index(Name) ->
  ibrowse:send_req(?URL ++ "/" ++ Name ++ "/", [], delete).

%% curl -XPOST 'http://localhost:9200/twitter/_refresh'
refresh_index(Name) ->
  ibrowse:send_req(?URL ++ "/" ++ Name ++ "/_refresh", [], post).

%% curl -XPUT 'http://localhost:9200/twitter/tweet/_mapping' -d '
%% {
%%     "tweet" : {
%%         "properties" : {
%%             "message" : {"type" : "string", "store" : "yes"}
%%         }
%%     }
%% }
%% '
put_mapping(Index, Type, Config) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/_mapping", [], put, Config).

%% curl -XPUT 'http://localhost:9200/twitter/tweet/1' -d '
%% {
%%     "user" : "kimchy",
%%     "postDate" : "2009-11-15T14:12:12",
%%     "message" : "trying out Elastic Search"
%% }
%% '
index(Index, Type, Id, Doc) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/" ++ Id, [], put, Doc).

%% curl -XGET 'http://localhost:9200/twitter/tweet/1'
get(Index, Type, Id) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/" ++ Id, [], get).

%% curl -XDELETE 'http://localhost:9200/twitter/tweet/1'
delete(Index, Type, Id) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/" ++ Id, [], delete).

%% curl -XGET 'http://localhost:9200/twitter/tweet/_search?q=user:kimchy'
search(Index, Type, Query) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/_search?q=" ++ Query, [], get).

search_dsl(Index, Type, Query) ->
  ibrowse:send_req(?URL ++ "/" ++ Index ++ "/" ++ Type ++ "/_search", [], get, Query).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_INDEX, "mc_riak_test_index").
-define(TEST_TYPE, "mc_riak_test_type").
-define(TEST_DOC_ID, "1").

reset() ->
  delete_index(?TEST_INDEX).

refresh_test_index() ->
  refresh_index(?TEST_INDEX).

create_test_index() ->
  {ok, "200", _, _} = create_index(?TEST_INDEX, "").

create_test_mapping() ->
  create_test_index(),
  RiakDocMapping = {struct, [{<<"type">>, <<"object">>}]},
  Json = mochijson2:encode(RiakDocMapping),
  {ok, "200", _, _} = put_mapping(?TEST_INDEX, ?TEST_TYPE, Json).

index_test_doc() ->
  create_test_mapping(),
  RiakDoc = {struct, [{<<"key">>, <<"value">>}]},
  Json = mochijson2:encode(RiakDoc),
  {ok, "200", _, _} = index(?TEST_INDEX, ?TEST_TYPE, ?TEST_DOC_ID, Json).

get_test_doc() ->
  index_test_doc(),
  refresh_test_index(),
  {ok, "200", _, _} = get(?TEST_INDEX, ?TEST_TYPE, ?TEST_DOC_ID).

mc_riak_doc_test_() ->
    {setup,
     fun() ->
         ibrowse:start()
     end,
     {generator,
     fun() ->
         case ibrowse:send_req(?URL, [], get) of
           {ok, "200", _, _} ->
             mc_riak_elastic_search_tests();
           _ ->
             {skipped, need_elastic_search_server}
         end
     end}}.

mc_riak_elastic_search_tests() ->
    [
    {"create_index",
     ?_test(
        begin
          reset(),
          create_test_index()
        end)},

     {"delete_index",
      ?_test(
         begin
           reset(),
           create_test_index(),
           {ok, "200", _, _} = delete_index(?TEST_INDEX)
         end)},

     {"refresh_index",
      ?_test(
         begin
           reset(),
           create_test_index(),
           {ok, "200", _, _} = refresh_index(?TEST_INDEX)
         end)},

     {"put_mapping",
     ?_test(
        begin
          reset(),
          create_test_mapping()
        end)},

     {"index",
      ?_test(
         begin
           reset(),
           index_test_doc()
         end)},

     {"get",
      ?_test(
         begin
           reset(),
           get_test_doc()
         end)},

     {"delete",
      ?_test(
         begin
           reset(),
           index_test_doc(),
           {ok, "200", _, _} = delete(?TEST_INDEX, ?TEST_TYPE, ?TEST_DOC_ID),
           refresh_test_index(),
           {ok, "404", _, _} = get(?TEST_INDEX, ?TEST_TYPE, ?TEST_DOC_ID)
         end)},

     {"search",
      ?_test(
         begin
           reset(),
           index_test_doc(),
           refresh_test_index(),
           {ok, "200", _, Body} = search(?TEST_INDEX, ?TEST_TYPE, "key:value"),
           {struct, Results} = mochijson2:decode(Body),
           {struct, HitResults} = proplists:get_value(<<"hits">>, Results),
           ?assertEqual(1, proplists:get_value(<<"total">>, HitResults))
         end)},

     {"search_dsl",
      ?_test(
         begin
           reset(),
           index_test_doc(),
           refresh_test_index(),
           Query = {struct, [{<<"query">>, {struct, [{<<"term">>, {struct, [{<<"key">>, <<"value">>}]}}]}}]},
           {ok, "200", _, Body} = search_dsl(?TEST_INDEX, ?TEST_TYPE, mochijson2:encode(Query)),
           {struct, Results} = mochijson2:decode(Body),
           {struct, HitResults} = proplists:get_value(<<"hits">>, Results),
           ?assertEqual(1, proplists:get_value(<<"total">>, HitResults))
         end)}
     ].

-endif.
