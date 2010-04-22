%%%-------------------------------------------------------------------
%%% @author Daniel Reverri <dan@appush.com>
%%% @copyright (C) 2010, Daniel Reverri
%%% @doc
%%% Read/Write functions for mc_riak_doc
%%%
%%% @end
%%% Created : 22 Apr 2010 by Daniel Reverri <dan@appush.com>
%%%-------------------------------------------------------------------
-module(mc_riak_doc_rw).

%% API
-export([write_json/3, read_json/1]).

%%%===================================================================
%%% API
%%%===================================================================
write_json(Object, Metadata, Doc) ->
  Value = encode_json(Doc),
  O1 = riakc_obj:update_value(Object, Value, <<"application/json">>),
  riakc_obj:update_metadata(O1, Metadata).

read_json(Object) ->
  {M, V} = hd(lists:sort(fun compare_content_dates/2, riakc_obj:get_contents(Object))),
  {M, decode_json(V)}.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% Encode the doc to JSON
%% @spec encode_json(Doc) -> JsonValue
encode_json(Doc) ->
  mochijson2:encode({struct, dict:to_list(Doc)}).

%% Decode the doc from JSON
%% @spec decode_json(Value) -> Doc
decode_json(Value) ->
  {struct, PropList} = mochijson2:decode(Value),
  dict:from_list(PropList).

%% date related functions below stolen from riak
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
