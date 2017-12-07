%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Nov 2017 10:18 AM
%%%-------------------------------------------------------------------
-module(transfer_data_handler).
-author("bgk").

%% API
-export([
    init/2,
    allowed_methods/2,
    content_types_provided/2,
    content_types_accepted/2,
    upload/2,
    download/2
]).

-include_lib("kernel/include/file.hrl").
-include_lib("database/include/obj.hrl").
-define(OBJECT_DIR, "/tmp/oids/").

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PUT">>], Req, State}.

content_types_provided(Req, State) ->
    {[{<<"application/octet-stream">>, download}], Req, State}.

content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"x-www-form-urlencoded">>, []}, upload}], Req, State}.

upload(Req, State) ->
    Oid = cowboy_req:binding(id, Req),
    {ok, IODevice} = file:open(?OBJECT_DIR ++ binary_to_list(Oid), [raw, binary, write]),
    case upload_stream(Req, IODevice, erlang:crc32(<<>>), 0) of
        {error, _Reason} ->
            Req1 = cowboy_req:set_resp_body(#{<<"reason">> => <<"interrupted!">>}, Req),
            Req2 = cowboy_req:reply(400, Req1),
            {stop, Req2, State};
        {ok, CRC32, ByteSize, Req1} ->
            riak_db:add_obj(binary_to_integer(Oid), ByteSize, CRC32),
            Req2 = cowboy_req:reply(200, Req1),
            {stop, Req2, State}
    end.

upload_stream(Req0, IODevice, CRC32, Size) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req} ->
            file:write(IODevice, Data),
            file:close(IODevice),
            {ok, erlang:crc32(CRC32, Data), Size + iolist_size(Data), Req};
        {more, Data, Req} ->
            file:write(IODevice, Data),
            file:sync(IODevice),
            upload_stream(Req, IODevice, erlang:crc32(CRC32, Data), Size + iolist_size(Data));
        {error, Reason} ->
            file:close(IODevice),
            {error, Reason}
    end.

download(Req, State) ->
    Oid = cowboy_req:binding(id, Req),
    Filename = ?OBJECT_DIR ++ binary_to_list(Oid),
    case file:read_file_info(Filename) of
        {error, _} ->
            Req1 = cowboy_req:set_resp_body(#{<<"reason">> => <<"oid object not exsist!">>}, Req),
            Req2 = cowboy_req:reply(400, Req1),
            {stop, Req2, State};
        {ok, #file_info{size = Size}} ->
            FileRecord = riak_db:get_obj(binary_to_integer(Oid)),
            case FileRecord of
                undefined ->
                    Req1 = cowboy_req:set_resp_body(#{<<"reason">> => <<"oid record not exsist!">>}, Req),
                    Req2 = cowboy_req:reply(400, Req1),
                    {stop, Req2, State};
                _ ->
                    lager:info("file size:~p, crc32c:~p, create_date:~p",
                        [FileRecord#obj.byte_size, FileRecord#obj.crc32c, FileRecord#obj.create_date]),
                    Req1 = cowboy_req:reply(200, #{}, {sendfile, 0, Size, Filename}, Req),
                    {stop, Req1, State}
            end
    end.


