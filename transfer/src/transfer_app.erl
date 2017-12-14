%%%-------------------------------------------------------------------
%% @doc transfer public API
%% @end
%%%-------------------------------------------------------------------

-module(transfer_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    Routes = [
        {"/api/obj/:id", transfer_server, #{}}
    ],
    Dispatch = cowboy_router:compile([
        %% {HostMatch, list({PathMatch, Handler, InitialState})}
        {'_', Routes}
    ]),
    {ok, _} = cowboy:start_clear(my_http_listener,
        [{port, 8080}],
        #{
            env => #{dispatch => Dispatch}
        }
    ),
    application:ensure_all_started(database),
    application:ensure_all_started(gun),
    transfer_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
