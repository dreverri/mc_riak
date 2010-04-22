{application, mc_riak,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             mc_riak_app,
             mc_riak_sup,
             mc_riak_client,
	     mc_riak_obj,
	     mochijson2
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { mc_riak_app, []}},
  {env, []}
 ]}.
