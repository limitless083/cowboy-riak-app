%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2017 3:25 PM
%%%-------------------------------------------------------------------
-module(riak_common).
-author("bgk").

%% API
-export([
    add_record_with_index/5,
    add_record_without_index/4,
    get_record/3,
    find_records/2,
    find_records/4,
    find_records_range/5,
    find_keys/4,
    delete_record/3,
    delete_records/2,
    delete_records/4,
    update_index/2,
    format_date_time/1
]).
-include_lib("riakc/include/riakc.hrl").

-spec add_record_with_index(pid(), bucket(), binary(), term(), [term()]) -> ok | {error, term()}.
add_record_with_index(Pid, Bucket, Key, Record, Indexes) ->
    Obj = riakc_obj:new(Bucket, Key, Record),
    Obj2 = update_index(Obj, Indexes),
    riakc_pb_socket:put(Pid, Obj2).

-spec add_record_without_index(pid(), bucket(), key(), term()) -> ok | {error, term()}.
add_record_without_index(Pid, Bucket, Key, Record) ->
    Obj = riakc_obj:new(Bucket, Key, Record),
    riakc_pb_socket:put(Pid, Obj).

-spec get_record(pid(), bucket() | bucket_and_type(), key()) -> term() | undefined.
get_record(Pid, Bucket, Key) ->
    case riakc_pb_socket:get(Pid, Bucket, Key) of
        {error, notfound} ->
            undefined;
        {ok, Result} ->
            binary_to_term(riakc_obj:get_value(Result))
    end.

-spec delete_record(pid(), bucket() | bucket_and_type(), key()) -> ok | {error, term()}.
delete_record(Pid, Bucket, Key) ->
    riakc_pb_socket:delete(Pid, Bucket, Key).

-spec delete_records(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer()) -> ok.
delete_records(Pid, Bucket, IndexT, IndexV) ->
    Keys = find_keys(Pid, Bucket, IndexT, IndexV),
    lists:foreach(
        fun(Key) ->
            riakc_pb_socket:delete(Pid, Bucket, Key)
        end,
        Keys).

-spec delete_records(pid(), bucket() | bucket_and_type()) -> ok.
delete_records(Pid, Bucket) ->
    case riakc_pb_socket:list_keys(Pid, Bucket) of
        {error, _} ->
            ok;
        {ok, Keys} ->
            lists:foreach(
                fun(Key) ->
                    delete_record(Pid, Bucket, Key)
                end,
                Keys),
            ok
    end.

-spec find_records(pid(), bucket() | bucket_and_type()) -> [term()] | [].
find_records(Pid, Bucket) ->
    case riakc_pb_socket:list_keys(Pid, Bucket) of
        {error, _} ->
            [];
        {ok, Keys} ->
            lists:map(
                fun(Key) ->
                    {ok, R} = riakc_pb_socket:get(Pid, Bucket, Key),
                    binary_to_term(riakc_obj:get_value(R))
                end,
                Keys
            )
    end.

-spec find_records(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer()) -> [term()] | [].
find_records(Pid, Bucket, IndexT, IndexV) ->
    case riakc_pb_socket:get_index(Pid, Bucket, IndexT, IndexV) of
        {error, _} ->
            [];
        {ok, Results} ->
            Keys = element(2, Results),
            lists:map(fun(Key) ->
                {ok, R} = riakc_pb_socket:get(Pid, Bucket, Key),
                binary_to_term(riakc_obj:get_value(R))
                      end, Keys)
    end.

-spec find_records_range(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer(), key() | integer()) -> [term()] | [].
find_records_range(Pid, Bucket, IndexT, IndexV_Start, IndexV_End) ->
    case riakc_pb_socket:get_index_range(Pid, Bucket, IndexT, IndexV_Start, IndexV_End) of
        {error, _} ->
            [];
        {ok, Results} ->
            Keys = element(2, Results),
            lists:map(fun(Key) ->
                {ok, R} = riakc_pb_socket:get(Pid, Bucket, Key),
                binary_to_term(riakc_obj:get_value(R))
                      end, Keys)
    end.

-spec find_keys(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer()) -> [term()] | [].
find_keys(Pid, Bucket, IndexT, IndexV) ->
    case riakc_pb_socket:get_index(Pid, Bucket, IndexT, IndexV) of
        {error, _} ->
            [];
        {ok, Results} ->
            Keys = element(2, Results),
            Keys
    end.

-spec update_index(term(), term() | [term()]) -> riakc_obj().
update_index(Obj, Indexes) ->
    MD1 = riakc_obj:get_update_metadata(Obj),
    MD2 = riakc_obj:set_secondary_index(
        MD1,
        Indexes),
    riakc_obj:update_metadata(Obj, MD2).

format_date_time(Time) ->
    case Time of
        undefined ->
            "";
        _ ->
            {{Y, M, D}, {H, MM, S}} = Time,
            integer_to_list(Y) ++ "-" ++ integer_to_list(M) ++ "-" ++ integer_to_list(D)
                ++ " " ++ integer_to_list(H) ++ ":" ++ integer_to_list(MM) ++ ":" ++ integer_to_list(S)
    end.


