%% @author Binbin Wang, Jing Luo
%% @doc This module is api for couchdb authencitation
%%		For CouchDB API, refer to, http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference


-module(couch_api_auth).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include("couchbeam.hrl").
-define(ApiTimeout, 30000).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([get_session/1,get_session/2,set_session/3,delete_session/2,user_add/4,user_delete/3]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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

%% API 
%% ====================================================================
%% Para: [{basic,true|false}]   default false
get_session(Server) ->
	get_session(Server,[{basic,false}]).
get_session(Server,Para) ->
    gen_server:call(?MODULE, {get_session, Server, Para}, ?ApiTimeout).

%% Name:string(), Password:string()
set_session(Server, Name, Password) ->
	gen_server:call(?MODULE, {set_session, Server, Name, Password}, ?ApiTimeout).

%% AuthSession:string()
delete_session(Server, AuthSession) ->
	gen_server:call(?MODULE, {delete_session, Server, AuthSession}, ?ApiTimeout).

%% UserName:string()
%% Roles:list()
%% Password:string()
user_add(Server,UserName,Roles,Password) ->
	gen_server:call(?MODULE, {user_add, Server, UserName, Roles, Password}, ?ApiTimeout).

%% UserName:string()
%% RevId:string
user_delete(Server,UserName,RevId) ->
	gen_server:call(?MODULE, {user_delete, Server, UserName,RevId}, ?ApiTimeout).

%% ====================================================================

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
handle_call({delete_session, Server, AuthSession}, _From, State) ->
    {reply, delete_session_internal(Server,AuthSession), State};

handle_call({get_session, Server, Para}, _From, State) ->
    {reply, get_session_internal(Server,Para), State};

handle_call({set_session, Server, Name, Password}, _From, State) ->
    {reply, set_session_internal(Server, Name, Password), State};

handle_call({user_add, Server, UserName,Roles,Password}, _From, State) ->
    {reply, user_add_internal(Server, UserName,Roles,Password), State};

handle_call({user_delete, Server, UserName,RevId}, _From, State) ->
    {reply, user_delete_internal(Server, UserName,RevId), State}.


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
get_session_internal(#server{options=IbrowseOpts}=Server,Para) ->
	Url = couchbeam_util:make_url(Server,["_session"],Para),
	case couchbeam_httpc:request(get, Url, ["200"], IbrowseOpts) of
		{ok, _, _, RespBody} ->
            {ok, couchbeam_ejson:decode(RespBody)};
        Error ->
            Error
    end.
set_session_internal(#server{options=IbrowseOpts}=Server,Name,Password) ->
	Url = couchbeam_util:make_url(Server,["_session"],[]),
	Body = "name="++Name++"&password="++Password,
	Headers = [{"Content-Type","application/x-www-form-urlencoded"}],
	case couchbeam_httpc:request(post, Url, ["200"], IbrowseOpts,Headers,Body) of
		{ok, _, RespHeaders, RespBody} ->
			{_, AuthSession} = lists:keyfind("Set-Cookie", 1, RespHeaders),
			{ok,couchbeam_doc:extend( {<<"Set-Cookie">>, list_to_binary(AuthSession)},couchbeam_ejson:decode(RespBody) )};
        Error ->
            Error
    end.

delete_session_internal(#server{options=IbrowseOpts}=Server,AuthSessionID) ->
	Url = couchbeam_util:make_url(Server,["_session"],[]),
	Headers = [{"AuthSession",AuthSessionID}],
	case couchbeam_httpc:request(delete, Url, ["200"], IbrowseOpts,Headers,[]) of
		{ok, _, _, RespBody} ->
            {ok, couchbeam_ejson:decode(RespBody)};
        Error ->
            Error
    end.

user_add_internal(#server{options=IbrowseOpts}=Server,UserName, Roles,Password) ->
	%%refer to : http://wiki.apache.org/couchdb/Security_Features_Overview#Authentication_database
	PassSha = couchbeam_util:to_hex(crypto:sha(Password)),
	
	[_A,Md]=string:tokens(os:cmd("openssl rand 16 | openssl md5")," "),
    Md5=string:substr(Md,1,32),
    Msg="foobar"++Md5,
    [_B,Re]=string:tokens(os:cmd("echo -n "++Msg++" | openssl sha1")," "),
    Salt=string:substr(Re,1,40),

	Url = couchbeam_util:make_url(Server,["_users","/","org.couchdb.user:"++UserName],[]),
	Headers = [{"Content-Type","application/json"}],
	Body=binary_to_list(couchbeam_ejson:encode(
		  {[{<<"type">>,<<"user">>},
          {<<"name">>,list_to_binary(UserName)},
          {<<"roles">>,lists:map(fun(X) -> list_to_binary(X) end, Roles)},
          {<<"password_sha">>,list_to_binary(PassSha)},
		  {<<"salt">>,list_to_binary(Salt)} ]}
		)),
	io:format("Url=~p \nBody = ~p \n",[Url,Body]),
	case couchbeam_httpc:request(put, Url, ["201"], IbrowseOpts,Headers,Body) of
		{ok, _, _, RespBody} ->
            {ok, couchbeam_ejson:decode(RespBody)};
        Error ->
            Error
    end.

user_delete_internal(#server{options=IbrowseOpts}=Server,UserName,RevId) ->
	Url = couchbeam_util:make_url(Server,["_users","/","org.couchdb.user:"++UserName],[{"rev",RevId}]),
	case couchbeam_httpc:request(delete, Url, ["200"], IbrowseOpts) of
		{ok, _, _, RespBody} ->
            {ok, couchbeam_ejson:decode(RespBody)};
        Error ->
            Error
    end.
