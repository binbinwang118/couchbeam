%% @author Binbin Wang, Jing Luo
%% @doc This module is api for couchdb server
%%		For CouchDB API, refer to, http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference


-module(couch_api_server).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include("couchbeam.hrl").
-define(ApiTimeout, 30000).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, get_server_info/1, get_couchdb_icon/1, get_all_dbs/1, get_active_tasks/1]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_server_info(#server{options=IbrowseOpts}=Server) ->
	gen_server:call(?MODULE, {get_server_info, Server}, ?ApiTimeout).

get_couchdb_icon(#server{options=IbrowseOpts}=Server) ->
	gen_server:call(?MODULE, {get_couchdb_icon, Server}, ?ApiTimeout).

get_all_dbs(#server{options=IbrowseOpts}=Server) ->
	gen_server:call(?MODULE, {get_all_dbs, Server}, ?ApiTimeout).

get_active_tasks(#server{options=IbrowseOpts}=Server) ->
	gen_server:call(?MODULE, {get_active_tasks, Server}, ?ApiTimeout).
	

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
handle_call(Request, From, State) ->
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
%% @doc Get Information from the server
%% @spec server_info(server()) -> {ok, iolist()}
server_info_internal(#server{options=IbrowseOpts}=Server) ->
    Url = binary_to_list(iolist_to_binary(couchbeam_util:server_url(Server))),
    case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
        {ok, _Status, _Headers, Body} ->
            Version = couchbeam_ejson:decode(Body),
            {ok, Version};
        Error -> Error
    end.

%% @doc Special path for providing a site icon
%% @spec server_info(server()) -> {ok, iolist()}
couchdb_icon_internal(#server{options=IbrowseOpts}=Server)->
	Url = couchbeam_util:make_url(Server, "favicon.ico", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			Icon = Body;
		Error -> Error
	end.

%% @doc Returns a list of all databases on couchdb server
%% @spec server_info(server()) -> {ok, iolist()}
get_all_dbs_internal(#server{options=IbrowseOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, "_all_dbs", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			AllDbs = couchbeam_ejson:decode(Body),
			{ok, AllDbs};
		Error -> Error
	end.
	
%% @doc Returns a list of running tasks on couchdb server
%% @spec server_info(server()) -> {ok, iolist()}
get_active_tasks_internal(#server{options=IbrowseOpts}=Server) ->
	Url = couchbeam_util:make_url(Server, "_active_tasks", []),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _Status, _Headers, Body} ->
			ActiveTasks = couchbeam_ejson:decode(Body),
			{ok, ActiveTasks};
		Error -> Error
	end.
		
