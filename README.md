MC Riak
========
Wrapper around Riak protobuffs client

Document values are encoded in JSON. MC Riak assumes a document will have
siblings and currently uses the sibling most recently modified.

Usage:
-------

	%% Create a new document
	{ok, D} = mc_riak_doc:new(<<"B">>,<<"K">>),

	%% Set a document property
	ok = mc_riak_doc:set(D, <<"author">>, <<"Daniel Reverri">>),

	%% Set many properties from a proplist
	ok = mc_riak_doc:set_from_list(D,
	   [{<<"email">>, <<"dan@appush.com">>},{<<"other">>,<<"value">>}]),

	%% Save the document
	ok = mc_riak_doc:save(D),

	%% Open the document later
	{ok, D1} = mc_riak_doc:open(<<"B">>,<<"K">>),

	%% Read the document
	ok = mc_riak_doc:read(D1),

	%% Get a property from the document
	Author = mc_riak_doc:get(D1, <<"author">>),

	%% Delete the document
	ok = mc_riak_doc:delete(D1)
