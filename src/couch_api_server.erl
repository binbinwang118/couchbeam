%% @author Binbin Wang, Jing Luo
%% @doc This module is api for couchdb server
%%		For CouchDB API, refer to, http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference


-module(couch_api_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, 
		 code_change/3]).
-include("couchbeam.hrl").
-define(Timeout, 30000).
%% ====================================================================
%% couch_api_server API functions
%  http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
%% ====================================================================
-export([start_link/0, get_server_info/1, get_couchdb_icon/1, get_all_dbs/1, 
		 get_active_tasks/1, replicate/4, add_replicator/5, remove_replicator/3,
		 get_uuids/2, restart_couchdb/1, get_couchdb_stats/1, get_couchdb_log/1,
		 get_config/1, get_config_section/2, get_config_key/3, update_config_key/4,
		 delete_config_key/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Get Information from the server
%% @spec server_info(server()) -> {ok, iolist()}
get_server_info(#server{}=Server) ->
	gen_server:call(?MODULE, {get_server_info, Server}, ?Timeout).

%% @doc Special path for providing a site icon
%% @spec server_info(server()) -> {ok, iolist()}
get_couchdb_icon(#server{}=Server) ->
	gen_server:call(?MODULE, {get_couchdb_icon, Server}, ?Timeout).

%% @doc Returns a list of all databases on couchdb server
%% @spec server_info(server()) -> {ok, iolist()}
get_all_dbs(#server{}=Server) ->
	gen_server:call(?MODULE, {get_all_dbs, Server}, ?Timeout).

%% @doc Returns a list of running tasks on couchdb server
%% @spec server_info(server()) -> {ok, iolist()}
get_active_tasks(#server{}=Server) ->
	gen_server:call(?MODULE, {get_active_tasks, Server}, ?Timeout).
	
%% @doc handle Replication. Allows to pass options with source and
%% target.  Options is a Json object.
%% ex:
%% ```
%% Source = "repldb1",
%% Target = "repldb2",
%% Prop = {[{<<"create_target">>, true}]},
%% '''
replicate(#server{}=Server, Source, Target, {Prop}) ->
	gen_server:call(?MODULE, {replicate, Server, Source, Target, {Prop}}, ?Timeout).

%% @doc remove the replication doc from replicator DB
%% ```
%% ex:
%% RepDocId = "docid123",
%% Source = "replidb1",
%% Target = "replidb2",
%% Prop = [{<<"create_target">>, true}],
%% '''
add_replicator(#server{}=Server, RepDocId, Source, Target, {Prop})->
	gen_server:call(?MODULE, {add_replicator, Server, RepDocId, Source, Target, {Prop}}, ?Timeout).

%% @doc remove the replication doc from replicator DB
%% ```
%% ex:
%% RepDocId = "docid123",
%% Rev = {<<"rev">>, <<"5-6b6c68009663774d697be8af974cdc4e">>},
%% '''
remove_replicator(#server{}=Server, RepDocId, Rev)->
	gen_server:call(?MODULE, {remove_replicator, Server, RepDocId, Rev}, ?Timeout).

%% @doc Returns a list of generated UUIDs from CouchDB
%% ```
%% ex:
%% Count = 5,
%% '''
%% @spec get_uuids(server(), integer()) -> lists()
get_uuids(#server{}=Server, Count) ->
    gen_server:call(?MODULE, {get_uuids, Server, Count}).

%% @doc Restart the couchdb server, requires admin privileges
restart_couchdb(#server{}=Server) ->
	gen_server:call(?MODULE, {restart_couchdb, Server}).

%% @doc Returns couchdb server statistics
get_couchdb_stats(#server{}=Server) ->
	gen_server:call(?MODULE, {get_couchdb_stats, Server}, ?Timeout).

%% @doc Returns the tail of the server's log file, requires admin privileges
get_couchdb_log(#server{}=Server) ->
	gen_server:call(?MODULE, {get_couchdb_log, Server}, ?Timeout).

%% @doc Returns the entire couchdb server configuration
get_config(#server{}=Server) ->
	gen_server:call(?MODULE, {get_config, Server}, ?Timeout).

%% @doc Returns a single section from couchdb server configuration
%% ```
%% ex:
%% Section = "couchdb",
%% '''
get_config_section(#server{}=Server, Section) ->
	gen_server:call(?MODULE, {get_config_section, Server, Section}, ?Timeout).

%% @doc Returns a single section from couchdb server configuration
%% ```
%% ex:
%% Section = "couchdb",
%% Key = "database_dir",
%% '''
get_config_key(#server{}=Server, Section, Key) ->
	gen_server:call(?MODULE, {get_config_key, Server, Section, Key}, ?Timeout).

%% @doc Set a single configuration value in a given section to couchdb server configuration
%% ```
%% ex:
%% Section = "couchdb",
%% Key = "database_dir",
%% UpdatedValue = "config_value",
%% '''
update_config_key(#server{}=Server, Section, Key, UpdatedValue) ->
	gen_server:call(?MODULE, {update_config_key, Server, Section, Key, UpdatedValue}, ?Timeout).

%% @doc Delete a single configuration value from a given section in couchdb server configuration
%% ```
%% ex:
%% Section = "couchdb",
%% Key = "database_dir",
%% '''
delete_config_key(#server{}=Server, Section, Key) ->
	gen_server:call(?MODULE, {delete_config_key, Server, Section, Key}).
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
handle_call({get_server_info, S}, _From, State) ->
	Result = server_info_internal(S),
	{reply, Result, State};
handle_call({get_couchdb_icon, S}, _From, State) ->
	Result = couchdb_icon_internal(S),
	{reply, Result, State};
handle_call({get_all_dbs, S}, _From, State) ->
	Result = get_all_dbs_internal(S),
	{reply, Result, State};
handle_call({get_active_tasks, S}, _From, State) ->
	Result = get_active_tasks_internal(S),
	{reply, Result, State};
handle_call({replicate, Server, Source, Target, {Prop}}, _From, State) ->
	Result = replicate_internal(Server, Source, Target, {Prop}),
	{reply, Result, State};
handle_call({add_replicator, Server, RepDocId, Source, Target, {Prop}}, _From, State) ->
	Result = add_replicator_internal(Server, RepDocId, Source, Target, {Prop}),
	{reply, Result, State};
handle_call({remove_replicator, Server, RepDocId, Rev}, _From, State) ->
	Result = remove_replicator_internal(Server, RepDocId, Rev),
	{reply, Result, State};
handle_call({get_uuids, Server, Count}, _From, State) ->
	Result = get_uuids_internal(Server, Count),
	{reply, Result, State};
handle_call({restart_couchdb, Server}, _Form, State) ->
	Result = restart_couchdb_internal(Server),
	{reply, Result, State};
handle_call({get_couchdb_stats, Server}, _From, State) ->
	Result = get_couchdb_stats_internal(Server),
	{reply, Result, State};
handle_call({get_couchdb_log, Server}, _From, State) ->
	Result = get_couchdb_log_internal(Server),
	{reply, Result, State};
handle_call({get_config, Server}, _From, State) ->
	Result = get_config_internal(Server),
	{reply, Result, State};
handle_call({get_config_section, Server, Section}, _From, State) ->
	Result = get_config_section_internal(Server, Section),
	{reply, Result, State};
handle_call({get_config_key, Server, Section, Key}, _From, State) ->
	Result = get_config_key_internal(Server, Section, Key),
	{reply, Result, State};
handle_call({update_config_key, Server, Section, Key, UpdatedValue}, _From, State) ->
	Result = update_config_key_internal(Server, Section, Key, UpdatedValue),
	{reply, Result, State};
handle_call({delete_config_key, Server, Section, Key}, _From, State) ->
	Result = delete_config_key_internal(Server, Section, Key),
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
server_info_internal(#server{options=IbrowseOpts}=Server) ->
    Url = binary_to_list(iolist_to_binary(couchbeam_util:server_url(Server))),
    case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
        {ok, _Status, _Headers, Body} ->
            Version = couchbeam_ejson:decode(Body),
            {ok, Version};
        Error -> Error
    end.

couchdb_icon_internal(#server{options=IbrowseOpts}=Server)->
	Url = couchbeam_util:make_url(Server, "favicon.ico", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			Icon = Body;
		Error -> Error
	end.

get_all_dbs_internal(#server{options=IbrowseOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, "_all_dbs", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			AllDbs = couchbeam_ejson:decode(Body),
			{ok, AllDbs};
		Error -> Error
	end.
	
get_active_tasks_internal(#server{options=IbrowseOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, "_active_tasks", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			ActiveTasks = couchbeam_ejson:decode(Body),
			{ok, ActiveTasks};
		Error -> Error
	end.
		
replicate_internal(Server, Source, Target, {Prop}) ->
    RepProp = [
        {<<"source">>, couchbeam_util:to_binary(Source)},
        {<<"target">>, couchbeam_util:to_binary(Target)} |Prop
    ],
    replicate_internal(Server, {RepProp}).

%% @doc Handle replication. Pass an object containting all informations
%% It allows to pass for example an authentication info
%% ```
%% RepObj = {[
%% {<<"source">>, <<"sourcedb">>},
%% {<<"target">>, <<"targetdb">>},
%% {<<"create_target">>, true}
%% ]}
%% replicate(Server, RepObj).
%% '''
%%
%% @spec replicate(Server::server(), RepObj::{list()})
%%          -> {ok, Result}|{error, Error}
replicate_internal(#server{options=IbrowseOpts}=Server, RepObj) ->
    Url = couchbeam_util:make_url(Server, "_replicate", []),
    Headers = [{"Content-Type", "application/json"}],
    JsonObj = couchbeam_ejson:encode(RepObj),
     case couchbeam_httpc:request(post, Url, ["200", "201"], IbrowseOpts,
                                  Headers, JsonObj) of
        {ok, _, _, Body} ->
            Res = couchbeam_ejson:decode(Body),
            {ok, Res};
        Error ->
            Error
    end.

add_replicator_internal(#server{options=IbrowseOpts}=Server, RepDocId, Source, Target, {Prop}) ->
	Url = couchbeam_util:make_url(Server, ["_replicator", "/", RepDocId], []),
	Headers = [{"Content-Type", "application/json"}],
	ReplicatorDoc = [
		{<<"_id">>, couchbeam_util:to_binary(RepDocId)},
        {<<"source">>, couchbeam_util:to_binary(Source)},
        {<<"target">>, couchbeam_util:to_binary(Target)} |Prop
    ],
	ReplicatorDocJsonObj = couchbeam_ejson:encode({ReplicatorDoc}),
	case couchbeam_httpc:request(put, Url, ["200", "201"], IbrowseOpts,
                                  Headers, ReplicatorDocJsonObj) of
        {ok, _, _, Body} ->
            JsonBody = couchbeam_ejson:decode(Body),
			{ok, JsonBody};
        Error ->
            Error
    end.

remove_replicator_internal(#server{options=IbrowseOpts}=Server, RepDocId, Rev) ->
	Url = couchbeam_util:make_url(Server, ["_replicator", "/", RepDocId], [Rev]),
	case couchbeam_httpc:request(delete, Url, ["200", "201"], IbrowseOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

get_uuids_internal(#server{options=IbrowseOpts}=Server, Count) ->
	Pros = {<<"count">>, Count},
	Url = couchbeam_util:make_url(Server, ["_uuids"], [Pros]),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowseOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.
restart_couchdb_internal(#server{options=IbrowserOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, ["_restart"], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(post, Url, ["200", "202"], IbrowserOpts, Headers) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

get_couchdb_stats_internal(#server{options=IbrowserOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, ["_stats"], []),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

get_couchdb_log_internal(#server{options=IbrowserOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, ["_log"], []),
	case couchbeam_httpc:request(get, Url, ["200", [201]], IbrowserOpts) of
		{ok, _, _, Body} ->
			Body;
	Error ->
			Error
	end.

get_config_internal(#server{options=IbrowserOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, ["_config"], []),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

get_config_section_internal(#server{options=IbrowserOpts}=Server, Section) ->
	Url = couchbeam_util:make_url(Server, ["_config", "/", Section], []),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

get_config_key_internal(#server{options=IbrowserOpts}=Server, Section, Key) ->
	Url = couchbeam_util:make_url(Server, ["_config", "/", Section, "/", Key], []),
	case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowserOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.

update_config_key_internal(#server{options=IbrowserOpts}=Server, Section, Key, UpdatedValue) ->
	Url = couchbeam_util:make_url(Server, ["_config", "/", Section, "/", Key], []),
	Headers = [{"Content-Type", "application/json"}],
	case couchbeam_httpc:request(put, Url, ["200"], IbrowserOpts, Headers, UpdatedValue) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error 			 ->
			Error
	end.

delete_config_key_internal(#server{options=IbrowseOpts}=Server, Section, Key) ->
	Url = couchbeam_util:make_url(Server, ["_config", "/", Section, "/", Key], []),
	case couchbeam_httpc:request(delete, Url, ["200", "201"], IbrowseOpts) of
		{ok, _, _, Body} ->
			JsonBody = couchbeam_ejson:decode(Body);
		Error ->
			Error
	end.
	
