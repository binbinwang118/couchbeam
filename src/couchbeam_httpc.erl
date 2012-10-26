%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license.
%%% See the NOTICE for more information.

-module(couchbeam_httpc).

-include_lib("ibrowse/include/ibrowse.hrl").

-export([request/4, request/5, request/6,
        request_stream/3, request_stream/4, request_stream/4,
        redirect_url/2]).

-define(TIMEOUT, infinity).

%% @doc send an ibrowse request
request(Method, Url, Expect, Options) ->
    request(Method, Url, Expect, Options, [], []).
request(Method, Url, Expect, Options, Headers) ->
    request(Method, Url, Expect, Options, Headers, []).
request(Method, Url, Expect, Options, Headers, Body) ->
    Accept = {<<"Accept">>, <<"application/json, */*;q=0.9">>},
    {Headers1, Options1} = maybe_oauth_header(Method, Url, Headers, Options),
    case hackney:request(Method, Url, [Accept|Headers1], Body, Options1) of
        {ok, Status, RespHeaders, Client} ->
            {ok, RespBody, _} = hackney:body(Client),
            Resp = {ok, Status, RespHeaders, RespBody},
            case lists:member(Status, Expect) of
                true -> Resp;
                false -> {error, Resp}
            end;
        Error -> Error
    end.

%% @doc stream an ibrowse request
request_stream(Method, Url, Options) ->
    request_stream(Method, Url, Options, []).
request_stream(Method, Url, Options, Headers) ->
    request_stream(Method, Url, Options, Headers, []).
request_stream(Method, Url, Options, Headers, Body) ->
    {Headers1, Options1} = maybe_oauth_header(Method, Url, Headers,
        Options),

    %% bypass the pool:
    FinalOptions = proplists:delete(pool, Options1),
    hackney:request(Method, Url, Headers1, Body, FinalOptions).

maybe_oauth_header(Method, Url, Headers, Options) ->
    case couchbeam_util:get_value(oauth, Options) of
        undefined ->
            {Headers, Options};
        OauthProps ->
            Hdr = couchbeam_util:oauth_header(Url, Method, OauthProps),
            {[Hdr|Headers], proplists:delete(oauth, Options)}
    end.



redirect_url(RespHeaders, OrigUrl) ->
    MochiHeaders = mochiweb_headers:make(RespHeaders),
    Location = mochiweb_headers:get_value("Location", MochiHeaders),
    #url{
        host = Host,
        host_type = HostType,
        port = Port,
        path = Path,  % includes query string
        protocol = Proto
    } = ibrowse_lib:parse_url(Location),
    #url{
        username = User,
        password = Passwd
    } = ibrowse_lib:parse_url(OrigUrl),
    Creds = case is_list(User) andalso is_list(Passwd) of
    true ->
        User ++ ":" ++ Passwd ++ "@";
    false ->
        []
    end,
    HostPart = case HostType of
    ipv6_address ->
        "[" ++ Host ++ "]";
    _ ->
        Host
    end,
    atom_to_list(Proto) ++ "://" ++ Creds ++ HostPart ++ ":" ++
        integer_to_list(Port) ++ Path.
