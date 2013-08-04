%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_sup).
-author('Benoît Chesneau <benoitc@e-engura.org>').
-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(SERVER, ?MODULE).


start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
	CouchApiChildSpec = 
		{{one_for_one, 10, 3600},
		 	[{couch_api_server,
			  	{couch_api_server, start_link, []},
			  	permanent,
			  	2000,
			  	worker,
			  	[couch_api_server]},
			 {couch_api_auth,
			  	{couch_api_auth, start_link, []},
			  	permanent,
			  	2000,
			  	worker,
			  	[couch_api_auth]},
			 {couch_api_db,
			  	{couch_api_db, start_link, []},
			  	permanent,
			  	2000,
			  	worker,
			  	[couch_api_db]},
			 {couch_api_doc,
			  	{couch_api_doc, start_link, []},
			  	permanent,
			  	2000,
			  	worker,
			  	[couch_api_doc]},
			 {couch_api_view,
			  	{couch_api_view, start_link, []},
			  	permanent,
			  	2000,
			  	worker,
			  	[couch_api_view]},
			 {couch_api_search,
			  	{couch_api_search, start_link, []},
				permanent,
				2000,
				worker,
				[couch_api_search]}
		 	]},
	{ok, CouchApiChildSpec}.
