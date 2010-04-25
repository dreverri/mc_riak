{application, mc_riak,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             mc_riak_app,
             mc_riak_sup,
             mc_riak_client,
	     mc_riak_doc,
	     mc_riak_doc_rw,
	     mc_riak_doc_list
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { mc_riak_app, []}},
  {env, []}
 ]}.
