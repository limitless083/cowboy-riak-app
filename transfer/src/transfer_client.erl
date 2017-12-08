%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2017 4:55 PM
%%%-------------------------------------------------------------------
-module(transfer_client).
-author("bgk").

%% API
-export([send_file/4]).

send_file(Host, Port, Route, Filepath) ->
    {ok, ConnPid} = gun:open(Host, Port),
    MRef = monitor(process, ConnPid),
    {ok, IoDevice} = file:open(Filepath, [read, binary, raw]),
    StreamRef = gun:put(ConnPid, Route, [
        {<<"content-type">>, <<"application/x-www-form-urlencoded">>},
        {<<"user-agent">>, "internal"}
    ]),
    do_sendfile(ConnPid, StreamRef, IoDevice),
    get_response(MRef, ConnPid, StreamRef).

do_sendfile(ConnPid, StreamRef, IoDevice) ->
    case file:read(IoDevice, 8000) of
        eof ->
            gun:data(ConnPid, StreamRef, fin, <<>>),
            file:close(IoDevice);
        {ok, Bin} ->
            gun:data(ConnPid, StreamRef, nofin, Bin),
            do_sendfile(ConnPid, StreamRef, IoDevice)
    end.

get_response(MRef, ConnPid, StreamRef) ->
    receive
        {gun_response, ConnPid, StreamRef, fin, 200, _Headers} ->
            gun:close(ConnPid),
            ok;
        {gun_response, ConnPid, StreamRef, fin, Status, _Headers} ->
            lager:error("error status:~p", [Status]),
            {error, error_status};
        {'DOWN', MRef, process, ConnPid, Reason} ->
            lager:error("error Reason:~p", [Reason]),
            {error, unexpected_down}
    end.