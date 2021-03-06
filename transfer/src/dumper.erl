%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Dec 2017 3:57 PM
%%%-------------------------------------------------------------------
-module(dumper).
-author("root").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(MINCOPIES, 1).
-define(WANTEDCOPIES, 1).

-record(state, {}).

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
    {ok, #state{}}.

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
handle_call({replicate, Route, Filepath}, From, State) ->
    spawn(
        fun() ->
            Result = replicate(?MINCOPIES, ?WANTEDCOPIES, Route, Filepath),
            gen_server:reply(From, Result)
        end
    ),
    {noreply, State};
handle_call({backend, Route, Filepath}, From, State) ->
    spawn(
        fun() ->
            Result = backend(Route, Filepath),
            gen_server:reply(From, Result)
        end
    ),
    {noreply, State};
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
replicate(MinCopies, WantedCopies, Route, Filepath) ->
    %% ensure WantedCopies is less than near transfer servers
    NearestTransferServers = nearest_transfer_servers(WantedCopies),
    lager:info("NearestTransferServers:~p", [NearestTransferServers]),
    pmap(
        fun({_Node, Host, Port}) ->
            transfer_client:send_file(Host, Port, Route, Filepath)
        end,
        NearestTransferServers,
        MinCopies,
        ok
    ).

backend(Route, Filepath) ->
    lager:info("cold backend:~p", [Route, Filepath]),
    {ok, completed}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
nearest_transfer_servers(N) ->
    {ok, CgriakList} = application:get_env(cgriak, cgriak_list),
    Index = get_index(lists:map(fun({X, _, _}) -> X end, CgriakList), node()),
    NearestIndexes = get_nearest_index(length(CgriakList), N, Index),
    lists:map(fun(X) -> lists:nth(X, CgriakList) end, NearestIndexes).

get_nearest_index(Len, N, Index) ->
    case Len =< N of
        true ->
            lists:seq(1, Len) -- [Index];
        false ->
            %% index
            lists:map(
                fun(X) ->
                    X rem Len + 1
                end,
                lists:seq(Index, Index + N - 1)
            )
    end.

get_index(List, Element) ->
    get_index(List, Element, 1).

get_index(List, Element, Location) ->
    case Location > length(List) of
        true ->
            -1;
        false ->
            case lists:nth(Location, List) of
                Element ->
                    Location;
                _ ->
                    get_index(List, Element, Location + 1)
            end
    end.


pmap(F, L, NWanted, ExpectedRes) ->
    S = self(),
    Pids = [spawn_link(fun() -> S ! {pmap_result, catch F(I)} end) || I <- L],
    gather(length(Pids), 0, NWanted, ExpectedRes).

gather(0, HaveSatisfied, NWanted, _) when HaveSatisfied < NWanted ->
    {error, incompleted};
gather(_, HaveSatisfied, NWanted, _) when HaveSatisfied >= NWanted ->
    {ok, completed};
gather(TotalPids, HaveSatisfied, NWanted, ExpectedRes) ->
    receive
        {pmap_result, Result} ->
            case Result of
                ExpectedRes ->
                    gather(TotalPids - 1, HaveSatisfied + 1, NWanted, ExpectedRes);
                _ ->
                    gather(TotalPids - 1, HaveSatisfied, NWanted, ExpectedRes)
            end
    end.
