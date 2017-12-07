%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Dec 2017 11:18 AM
%%%-------------------------------------------------------------------
-module(riak_db).
-author("bgk").

-behaviour(gen_server).

%% API
-export([
    add_obj/3,
    get_obj/1,
    find_obj/2,
    find_obj_range/3,
    find_obj/0,
    update_obj/3,
    delete_obj/1,
    delete_objs/0
]).

%% internal api
-export([
    add_obj_i/4,
    get_obj_i/2,
    find_obj_i/3,
    find_obj_range_i/4,
    find_obj_i/1,
    update_obj_i/4,
    delete_obj_i/2,
    delete_objs_i/1
]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    riakc_pid :: pid()
}).
-include("../include/obj.hrl").
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    {ok, #state{riakc_pid = Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({F, A}, _From, State = #state{riakc_pid = RiakcPid}) ->
    Reply = apply(?MODULE, F, [RiakcPid | A]),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_obj_i(Pid, Oid, ByteSize, CRC32C) ->
    Record = #obj{id = Oid, byte_size = ByteSize, crc32c = CRC32C, create_date = erlang:universaltime()},
    Indexes = [{{integer_index, "id"}, [Oid]},
        {{binary_index, "create_date"}, [riak_common:format_date_time(erlang:universaltime())]}],
    riak_common:add_record_with_index(Pid, <<"obj">>, integer_to_binary(Record#obj.id), Record, Indexes).

get_obj_i(Pid, Oid) ->
    riak_common:get_record(Pid, <<"obj">>, integer_to_binary(Oid)).

find_obj_i(Pid, by_create_date, CreateDate) ->
    riak_common:find_records(Pid, <<"obj">>, {binary_index, "create_date"}, CreateDate).

find_obj_range_i(Pid, by_create_date, CreateDate1, CreateDate2) ->
    riak_common:find_records_range(Pid, <<"obj">>, {binary_index, "create_date"}, CreateDate1, CreateDate2).

find_obj_i(Pid) ->
    riak_common:find_records(Pid, <<"obj">>).

update_obj_i(Pid, create_date, Oid, New) ->
    case riakc_pb_socket:get(Pid, <<"obj">>, integer_to_binary(Oid)) of
        {error, not_found} ->
            {error, not_found};
        {ok, Result} ->
            Record = binary_to_term(riakc_obj:get_value(Result)),
            UpdatedObj = riakc_obj:update_value(Result, Record#obj{create_date = New}),
            UpdatedObj2 = riak_common:update_index(UpdatedObj, [{{integer_index, "id"}, [Record#obj.id]},
                {{binary_index, "create_date"}, [riak_common:format_date_time(erlang:universaltime())]}]),
            {ok, NewestObj} = riakc_pb_socket:put(Pid, UpdatedObj2, [return_body]),
            NewestObj
    end.

delete_obj_i(Pid, Oid) ->
    riak_common:delete_record(Pid, <<"obj">>, integer_to_binary(Oid)).

delete_objs_i(Pid) ->
    riak_common:delete_records(Pid, <<"obj">>).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% External functions
add_obj(Oid, ByteSize, CRC32C) ->
    catch gen_server:call(?MODULE, {add_obj_i, [Oid, ByteSize, CRC32C]}).

get_obj(Oid) ->
    catch gen_server:call(?MODULE, {get_obj_i, [Oid]}).

update_obj(create_date, Oid, NewCreateDate) ->
    catch gen_server:call(?MODULE, {update_obj_i, [create_date, Oid, NewCreateDate]}).

find_obj() ->
    catch gen_server:call(?MODULE, {find_obj_i, []}).

find_obj(by_create_date, CreateDate) ->
    catch gen_server:call(?MODULE, {find_obj_i, [by_create_date, CreateDate]}).

find_obj_range(by_create_date, CreateDate1, CreateDate2) ->
    catch gen_server:call(?MODULE, {find_obj_range_i, [by_create_date, CreateDate1, CreateDate2]}).

delete_obj(Oid) ->
    catch gen_server:call(?MODULE, {delete_obj_i, [Oid]}).

delete_objs() ->
    catch gen_server:call(?MODULE, {delete_objs_i, []}).

