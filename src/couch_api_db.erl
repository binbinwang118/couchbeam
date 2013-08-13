%% @author Binbin Wang, Jing Luo
%% @doc This module is api for couchdb database
%%		For CouchDB API, refer to, http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference


-module(couch_api_db).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include("couchbeam.hrl").
-define(Timeout, 30000).
%% ====================================================================
%% couch_api_db API functions
%% http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
%% ====================================================================
-export([start_link/0, get_db_info/2, create_db/2, delete_db/2, get_db_changes/2, get_db_changes/3,
		 compact_db/2, compact_view/3, cleanup_view/2, ensure_full_commit/2, bulk_docs/4]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Returns database information from couchdb server
%% ex:
%% ```
%% DbName= "db1",
%% '''
get_db_info(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {get_db_info, Server, DbName}, ?Timeout).

%% @doc Create a new database in couchdb server
%% ex:
%% ```
%% DbName= "db1",
%% '''
create_db(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {create_db, Server, DbName}, ?Timeout).

%% @doc Delete an existing database in couchdb server
%% ex:
%% ```
%% DbName= "db1",
%% '''
delete_db(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {delete_db, Server, DbName}, ?Timeout).

%% @doc Returns changes for the given database from couchdb server
%% ex:
%% ```
%% DbName= "db1",
%% '''
get_db_changes(#server{}=Server, DbName) ->
	get_db_changes(Server, DbName, []).

%% @doc Returns changes for the given database from couchdb server
%%		http://wiki.apache.org/couchdb/HTTP_database_API#Changes
%% ex:
%% ```
%% DbName = "db1",
%% Props = [{<<"include_docs">>, true}, {<<"since">>, 0}],
%% '''
get_db_changes(#server{}=Server, DbName, Props) ->
	gen_server:call(?MODULE, {get_db_changes, Server, DbName, Props}, ?Timeout).

%% @doc Starts a compaction for the database, requires admin privileges
%% ex:
%% ```
%% DbName= "db1",
%% '''
compact_db(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {compact_db, Server, DbName}, ?Timeout).

%% @doc Starts a compaction for all the views in the selected design document, requires admin privileges
%% ex:
%% ```
%% DbName= "db1",
%% DesignDoc = "exampledesigndoc",
%% '''
compact_view(#server{}=Server, DbName, DesignDoc) ->
	gen_server:call(?MODULE, {compact_view, Server, DbName, DesignDoc}, ?Timeout).

%% @doc Removes view files that are not used by any design document, requires admin privileges
%% ex:
%% ```
%% DbName= "db1",
%% '''
cleanup_view(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {cleanup_view, Server, DbName}, ?Timeout).
	
%% @doc Makes sure all uncommited changes are written and synchronized to the disk
%% ex:
%% ```
%% DbName= "db1",
%% '''
ensure_full_commit(#server{}=Server, DbName) ->
	gen_server:call(?MODULE, {ensure_full_commit, Server, DbName}, ?Timeout).

%% @doc Insert multiple documents in to the database in a single request
%% ex:
%% ```
%% DbName= "db1",
%% Doc1 = {[{<< "_id" >>, << "bulkdoc1" >> }]},
%% Doc2 = {[{<< "_id" >>, << "bulkdoc2" >> }]},
%% Doc3 = {[{<< "key3" >>, << "value3" >> }]},
%% DocArray = [Doc1, Doc2, Doc3],
%% AllOrNothing = [{"all_or_nothing", true}],
%% '''
bulk_docs(#server{}=Server, DbName, DocArray, AllOrNothing) ->
	gen_server:call(?MODULE, {bulk_docs, Server, DbName, DocArray, AllOrNothing}, ?Timeout).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {}).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([]) ->
	process_flag(trap_exit, true),
    {ok, #state{}}.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call({get_db_info, Server, DbName}, _From, State) ->
	Result = get_db_info_internal(Server, DbName),
	{reply, Result, State};
handle_call({create_db, Server, Dbname}, _From, State) ->
	Result = create_db_internal(Server, Dbname),
	{reply, Result, State};
handle_call({delete_db, Server, DbName}, _From, State) ->
	Result = delete_db_internal(Server, DbName),
	{reply, Result, State};
handle_call({get_db_changes, Server, DbName, Props}, _From, State) ->
	Result = get_db_changes_internal(Server, DbName, Props),
	{reply, Result, State};
handle_call({compact_db, Server, DbName}, _From, State) ->
	Result = compact_db_internal(Server, DbName),
	{reply, Result, State};
handle_call({compact_view, Server, DbName, DesignDoc}, _From, State) ->
	Result = compact_view_internal(Server, DbName, DesignDoc),
	{reply, Result, State};
handle_call({cleanup_view, Server, DbName}, _From, State) ->
	Result = cleanup_view_internal(Server, DbName),
	{reply, Result, State};
handle_call({ensure_full_commit, Server, DbName}, _From, State) ->
	Result = ensure_full_commit_internal(Server, DbName),
	{reply, Result, State};
handle_call({bulk_docs, Server, DbName, DocArray, AllOrNothing}, _From, State) ->
	Result = bulk_docs_internal(Server, DbName, DocArray, AllOrNothing),
	{reply, Result, State};
handle_call(Request, From, State) ->
	io:format("Default handl_call for debug!\n"),
    Reply = ok,
    {reply, Reply, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(Msg, State) ->
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(Info, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(OldVsn, State, Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
get_db_info_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName], []),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error 			 ->
			Error
	end.

create_db_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName], []),
	case couchbeam_httpc:request(put, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.

delete_db_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName], []),
	case couchbeam_httpc:request(delete, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			->
			Error
	end.

get_db_changes_internal(Server, DbName, Props) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName, "/", "_changes"], Props),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.
		
compact_db_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbroswerOpts}=Server, [DbName, "/", "_compact"], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["202"], IbroswerOpts, Headers) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.

compact_view_internal(Server, DbName, DesignDoc) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName, "/", "_compact", "/", DesignDoc], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["202"], IbrowserOpts, Headers) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.

cleanup_view_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName, "/", "_view_cleanup"], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["202"], IbrowserOpts, Headers) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.

ensure_full_commit_internal(Server, DbName) ->
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName, "/", "_ensure_full_commit"], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["201"], IbrowserOpts, Headers) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.
		
bulk_docs_internal(Server, DbName, DocArray, AllOrNothing) ->
	DocWithId = [add_id_to_doc(Server, Doc) || Doc <- DocArray],
	QueryBody = case couchbeam_util:get_value("all_or_nothing", AllOrNothing, false) of
		true ->
			couchbeam_ejson:encode({[
                {<<"all_or_nothing">>, true},
                {<<"docs">>, DocWithId}
            ]});
		_ 	->
			couchbeam_ejson:encode({[{<<"docs">>, DocWithId}]})
				end,
	Url = couchbeam_util:make_url(#server{options=IbrowserOpts}=Server, [DbName, "/", "_bulk_docs"], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["200", "201"], IbrowserOpts, Headers, QueryBody) of
		{ok, _, _, Body} ->
			couchbeam_ejson:decode(Body);
		Error			 ->
			Error
	end.

%% add missing docid to a list of documents if needed
add_id_to_doc(Server, {Doc}) ->
    case couchbeam_util:get_value(<<"_id">>, Doc) of
        undefined ->
            UUID = [couch_api_server:get_uuids(Server, 1)],
			[{[{<<"uuids">>,[DocId]}]}] = UUID,
            {[{<<"_id">>, DocId}|Doc]};
        _DocId ->
            {Doc}
    end.
	