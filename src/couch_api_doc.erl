%% @author Binbin Wang, Jing Luo
%% @doc This module is api for couchdb document
%%		For CouchDB API, refer to, http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference


-module(couch_api_doc).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include("couchbeam.hrl").
-define(ApiTimeout, 30000).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([open_doc/2,open_doc/3,save_doc_post/2,save_doc_post/3,
		 lookup_doc_rev/2,lookup_doc_rev/3,save_doc/2,save_doc/3,
		 delete_doc/2,copy_doc/3,copy_doc/4,put_attachment/4,put_attachment/5,
		 fetch_attachment/3,fetch_attachment/4,fetch_attachment/5]).

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
%% @doc save a document
%% @equiv save_doc(Db, Doc, [])
save_doc(Db, Doc) ->
    save_doc(Db, Doc, []).

%% @doc save a document
%% A document is a Json object like this one:
%%
%%      ```{[
%%          {<<"_id">>, <<"myid">>},
%%          {<<"title">>, <<"test">>}
%%      ]}'''
%%
%% Options are arguments passed to the request. This function return a
%% new document with last revision and a docid. If _id isn't specified in
%% document it will be created. Id is created by extracting an uuid from
%% the couchdb node.
%%
%% @spec save_doc(Db::db(), Doc, Options::list()) -> {ok, Doc1}|{error, Error}
save_doc(Db, Doc, Options) ->
	gen_server:call(?MODULE, {save_doc, Db, Doc, Options}, ?ApiTimeout).
	
save_doc_post(Db, Doc) ->
    save_doc_post(Db, Doc, []).
save_doc_post(Db, Doc, Options) ->
	gen_server:call(?MODULE, {save_doc_post, Db, Doc, Options}, ?ApiTimeout).

%% @doc open a document
%% @equiv open_doc(Db, DocId, [])
open_doc(Db, DocId) ->
    open_doc(Db, DocId, []).

%% @doc open a document
%% Params is a list of query argument. Have a look in CouchDb API
%% @spec open_doc(Db::db(), DocId::string(), Params::list())
%%          -> {ok, Doc}|{error, Error}
open_doc(Db, DocId, Params) ->
    gen_server:call(?MODULE, {open_doc, Db, DocId, Params}, ?ApiTimeout).

lookup_doc_rev(Db, DocId) ->
    lookup_doc_rev(Db, DocId, []).

lookup_doc_rev(Db, DocId, Params) ->
	gen_server:call(?MODULE, {lookup_doc_rev, Db, DocId, Params}, ?ApiTimeout).

%% @doc delete a document
%% if you want to make sure the doc it emptied on delete, use the option
%% {empty_on_delete,  true} or pass a doc with just _id and _rev
%% members.
%% @spec delete_doc(Db, Doc) -> {ok,Result}|{error,Error}
delete_doc(Db, Doc) ->
     gen_server:call(?MODULE, {delete_doc, Db, Doc}, ?ApiTimeout).

%% DocId:string() NewDocId:string() RevId:string()
copy_doc(Db,DocId,NewDocId) ->
	copy_doc(Db,DocId,NewDocId,undefined).
copy_doc(Db,DocId,OldDocId,RevId) ->
	gen_server:call(?MODULE, {copy_doc, Db,DocId,OldDocId,RevId}, ?ApiTimeout).

put_attachment(Db, DocId, Name, Body)->
    put_attachment(Db, DocId, Name, Body, []).

%% @doc put an attachment
%% @spec put_attachment(Db::db(), DocId::string(), Name::string(),
%%                      Body::body(), Option::optionList()) -> {ok, iolist()}
%%       optionList() = [option()]
%%       option() = {rev, string()} |
%%                  {content_type, string()} |
%%                  {content_length, string()}
%%       body() = [] | string() | binary() | fun_arity_0() | {fun_arity_1(), initial_state()}
%%       initial_state() = term()
put_attachment(Db, DocId, Name, Body, Options) ->
	gen_server:call(?MODULE, {put_attachment, Db, DocId, Name, Body, Options}, ?ApiTimeout).

%% @doc fetch a document attachment
%% @equiv fetch_attachment(Db, DocId, Name, [], DEFAULT_TIMEOUT)
fetch_attachment(Db, DocId, Name) ->
    fetch_attachment(Db, DocId, Name, [], ?DEFAULT_TIMEOUT).

%% @doc fetch a document attachment
%% @equiv fetch_attachment(Db, DocId, Name, Options, DEFAULT_TIMEOUT)
fetch_attachment(Db, DocId, Name, Options) ->
    fetch_attachment(Db, DocId, Name, Options, ?DEFAULT_TIMEOUT).

%% @doc fetch a document attachment
%% @spec fetch_attachment(db(), string(), string(),
%%                        list(), infinity|integer()) -> {ok, binary()}
fetch_attachment(Db, DocId, Name, Options, Timeout) ->
	gen_server:call(?MODULE, {fetch_attachment, Db, DocId, Name, Options, Timeout}, ?ApiTimeout).


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
handle_call({save_doc, Db, Doc, Options}, _From, State) ->
    {reply, save_doc_internal(Db, Doc, Options), State};

handle_call({save_doc_post, Db, Doc, Options}, _From, State) ->
    {reply, save_doc_post_internal(Db, Doc, Options), State};

handle_call({open_doc, Db, DocId, Params}, _From, State) ->
    {reply, open_doc_internal(Db, DocId, Params), State};

handle_call({lookup_doc_rev, Db, DocId, Params}, _From, State) ->
    {reply, lookup_doc_rev_internal(Db, DocId, Params), State};

handle_call({delete_doc, Db, Doc}, _From, State) ->
    {reply, delete_doc_internal(Db, Doc), State};

handle_call({copy_doc, Db,DocId,OldDocId,RevId}, _From, State) ->
    {reply, copy_doc_internal(Db,DocId,OldDocId,RevId), State};

handle_call({put_attachment, Db, DocId, Name, Body, Options}, _From, State) ->
    {reply, put_attachment_internal(Db, DocId, Name, Body, Options), State};

handle_call({fetch_attachment, Db, DocId, Name, Options, Timeout}, _From, State) ->
    {reply, fetch_attachment_internal(Db, DocId, Name, Options, Timeout), State}.

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
save_doc_post_internal(#db{server=Server, options=IbrowseOpts}=Db, {Props}=Doc, Options) ->
    Url = couchbeam_util:make_url(Server, couchbeam_util:db_url(Db), Options),
    Body = couchbeam_ejson:encode(Doc),
    Headers = [{"Content-Type", "application/json"}],
    case couchbeam_httpc:request(post, Url, ["201", "202"], IbrowseOpts, Headers, Body) of
        {ok, _, _, RespBody} ->
            {JsonProp} = couchbeam_ejson:decode(RespBody),
            NewRev = couchbeam_util:get_value(<<"rev">>, JsonProp),
            NewDocId = couchbeam_util:get_value(<<"id">>, JsonProp),
            Doc1 = couchbeam_doc:set_value(<<"_rev">>, NewRev,
                couchbeam_doc:set_value(<<"_id">>, NewDocId, Doc)),
            {ok, Doc1};
        Error ->
            Error
    end.

open_doc_internal(#db{server=Server, options=IbrowseOpts}=Db, DocId, Params) ->
    DocId1 = couchbeam_util:encode_docid(DocId),
    Url = couchbeam_util:make_url(Server, couchbeam_util:doc_url(Db, DocId1), Params),
    case couchbeam_httpc:request(get, Url, ["200", "201"], IbrowseOpts) of
        {ok, _, _, Body} ->
            {ok, couchbeam_ejson:decode(Body)};
        Error ->
            Error
    end.

lookup_doc_rev_internal(#db{server=Server, options=IbrowseOpts}=Db, DocId, Params) ->
    DocId1 = couchbeam_util:encode_docid(DocId),
    Url = couchbeam_util:make_url(Server, couchbeam_util:doc_url(Db, DocId1), Params),
    case couchbeam_httpc:request(head, Url, ["200"], IbrowseOpts) of
        {ok, _, Headers, _} ->
            MHeaders = mochiweb_headers:make(Headers),
            re:replace(mochiweb_headers:get_value("etag", MHeaders),
                <<"\"">>, <<>>, [global, {return, binary}]);
        Error ->
            Error
    end.

save_doc_internal(#db{server=Server, options=IbrowseOpts}=Db, {Props}=Doc, Options) ->
	DocId = case couchbeam_util:get_value(<<"_id">>, Props) of
        undefined ->
            {[{_Uid,[Id]}]} = couch_api_server:get_uuids(Server,1),
            binary_to_list(Id);
        DocId1 ->
            couchbeam_util:encode_docid(DocId1)
    end,
    Url = couchbeam_util:make_url(Server, couchbeam_util:doc_url(Db, DocId), Options),
    Body = couchbeam_ejson:encode(Doc),
    Headers = [{"Content-Type", "application/json"}],
    case couchbeam_httpc:request(put, Url, ["201", "202"], IbrowseOpts, Headers, Body) of
        {ok, _, _, RespBody} ->
            {JsonProp} = couchbeam_ejson:decode(RespBody),
            NewRev = couchbeam_util:get_value(<<"rev">>, JsonProp),
            NewDocId = couchbeam_util:get_value(<<"id">>, JsonProp),
            Doc1 = couchbeam_doc:set_value(<<"_rev">>, NewRev,
                couchbeam_doc:set_value(<<"_id">>, NewDocId, Doc)),
            {ok, Doc1};
        Error ->
            Error
    end.

delete_doc_internal(#db{server=Server, options=IbrowseOpts}=Db, {Props}=Doc) ->
	DocId=couchbeam_util:get_value(<<"_id">>, Props),
	Rev=couchbeam_util:get_value(<<"_rev">>, Props),
	Url = couchbeam_util:make_url(Server, couchbeam_util:doc_url(Db, DocId), [{"rev",binary_to_list(Rev)}]),
	case couchbeam_httpc:request(delete, Url, ["200"], IbrowseOpts) of
        {ok, _, _, RespBody} ->
			{ok, couchbeam_ejson:decode(RespBody)};
		Error ->
            Error
    end.

copy_doc_internal(#db{server=Server, options=IbrowseOpts}=Db, DocId,OldDocId,RevId) ->
	Url = couchbeam_util:make_url(Server,couchbeam_util:doc_url(Db, DocId),[]),
	Destination = case RevId of
		undefined -> OldDocId;
		Rev -> OldDocId++"?rev="++RevId
	end,
	Headers = [{"Destination",Destination}],
	case couchbeam_httpc:request(copy, Url, ["201"], IbrowseOpts, Headers, []) of
		{ok, _, _, RespBody} ->
            {ok, couchbeam_ejson:decode(RespBody)};
        Error ->
            Error
    end.

put_attachment_internal(#db{server=Server, options=IbrowseOpts}=Db, DocId, Name, Body, Options) ->
    QueryArgs = case couchbeam_util:get_value(rev, Options) of
        undefined -> [];
        Rev -> [{"rev", couchbeam_util:to_list(Rev)}]
    end,

    Headers = couchbeam_util:get_value(headers, Options, []),

    FinalHeaders = lists:foldl(fun(Option, Acc) ->
                case Option of
                    {content_length, V} -> [{"Content-Length", V}|Acc];
                    {content_type, V} -> [{"Content-Type", V}|Acc];
                    _ -> Acc
                end
        end, Headers, Options),

    Url = couchbeam_util:make_url(Server, [couchbeam_util:db_url(Db), "/",
            couchbeam_util:encode_docid(DocId), "/",
            couchbeam_util:encode_att_name(Name)], QueryArgs),

    case couchbeam_httpc:request(put, Url, ["201"], IbrowseOpts, FinalHeaders, Body) of
        {ok, _, _, RespBody} ->
            {[{<<"ok">>, true}|R]} = couchbeam_ejson:decode(RespBody),
            {ok, {R}};
        Error ->
            Error
    end.

fetch_attachment_internal(Db, DocId, Name, Options, Timeout) ->
    {ok, ReqId} = stream_fetch_attachment(Db, DocId, Name, self(), Options,
        Timeout),
    couchbeam_attachments:wait_for_attachment(ReqId, Timeout).

%% @doc stream fetch attachment to a Pid.
%% @equiv stream_fetch_attachment(Db, DocId, Name, ClientPid, [], DEFAULT_TIMEOUT)
stream_fetch_attachment(Db, DocId, Name, ClientPid) ->
    stream_fetch_attachment(Db, DocId, Name, ClientPid, [], ?DEFAULT_TIMEOUT).


%% @doc stream fetch attachment to a Pid.
%% @equiv stream_fetch_attachment(Db, DocId, Name, ClientPid, Options, DEFAULT_TIMEOUT)
stream_fetch_attachment(Db, DocId, Name, ClientPid, Options) ->
     stream_fetch_attachment(Db, DocId, Name, ClientPid, Options, ?DEFAULT_TIMEOUT).

%% @doc stream fetch attachment to a Pid. Messages sent to the Pid
%%      will be of the form `{reference(), message()}',
%%      where `message()' is one of:
%%      <dl>
%%          <dt>done</dt>
%%              <dd>You got all the attachment</dd>
%%          <dt>{ok, binary()}</dt>
%%              <dd>Part of the attachment</dd>
%%          <dt>{error, term()}</dt>
%%              <dd>n error occurred</dd>
%%      </dl>
%% @spec stream_fetch_attachment(Db::db(), DocId::string(), Name::string(),
%%                               ClientPid::pid(), Options::list(), Timeout::integer())
%%          -> {ok, reference()}|{error, term()}
stream_fetch_attachment(#db{server=Server, options=IbrowseOpts}=Db, DocId, Name, ClientPid,
        Options, Timeout) ->
    Options1 = couchbeam_util:parse_options(Options),
    %% custom headers. Allows us to manage Range.
    {Options2, Headers} = case couchbeam_util:get_value("headers", Options1) of
        undefined ->
            {Options1, []};
        Headers1 ->
            {proplists:delete("headers", Options1), Headers1}
    end,
    DocId1 = couchbeam_util:encode_docid(DocId),
    Url = couchbeam_util:make_url(Server, [couchbeam_util:db_url(Db), "/", DocId1, "/", Name], Options2),
    StartRef = make_ref(),
    Pid = spawn(couchbeam_attachments, attachment_acceptor, [ClientPid,
            StartRef, Timeout]),
    case couchbeam_httpc:request_stream(Pid, get, Url, IbrowseOpts, Headers) of
        {ok, ReqId}    ->
            Pid ! {ibrowse_req_id, StartRef, ReqId},
            {ok, StartRef};
        {error, Error} -> {error, Error}
    end.