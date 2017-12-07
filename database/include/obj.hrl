%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Dec 2017 6:54 PM
%%%-------------------------------------------------------------------
-author("bgk").

-record(obj, {
    id :: integer(),
    byte_size = 0 :: integer(),
    crc32c :: integer(),
    create_date :: term()
}).