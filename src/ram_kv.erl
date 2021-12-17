%% ==========================================================================================================
%% Ram - An ephemeral distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2021 Roberto Ostinelli <roberto@ostinelli.net>.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.
%% ==========================================================================================================
-module(ram_kv).

%% API
-export([get/1]).
-export([put/3]).
-export([delete/1]).

%% includes
-include("ram.hrl").

%% ===================================================================
%% API
%% ===================================================================
-spec get(Key :: term()) -> {ok, Value :: term(), Version :: term()}.
get(Key) ->
    F = fun() ->
        case mnesia:read({?TABLE, Key}) of
            [] -> {error, undefined};
            [#?TABLE{value = Value, version = Version}] -> {ok, Value, Version}
        end
    end,
    mnesia:activity(transaction, F).

-spec put(Key :: term(), Value :: term(), Version :: term()) -> {ok, Value :: term(), Version :: term()}.
put(Key, Value, Version) ->
    F = fun() ->
        VersionMatch = case mnesia:read({?TABLE, Key}) of
            [] ->
                case Version of
                    undefined -> ok;
                    _ -> {error, deleted}
                end;

            [#?TABLE{version = Version}] ->
                ok;

            _ ->
                {error, outdated}
        end,
        case VersionMatch of
            ok ->
                Version1 = generate_id(),
                mnesia:write(#?TABLE{
                    key = Key,
                    value = Value,
                    version = Version1
                }),
                {ok, Version1};

            {error, Reason} ->
                {error, Reason}
        end
    end,
    mnesia:activity(transaction, F).

-spec delete(Key :: term()) -> ok.
delete(Key) ->
    F = fun() ->
        case mnesia:read({?TABLE, Key}) of
            [] -> {error, undefined};
            _ -> mnesia:delete({?TABLE, Key})
        end
    end,
    mnesia:activity(transaction, F).

%% ===================================================================
%% Internal
%% =================================================
-spec generate_id() -> binary().
generate_id() ->
    binary:encode_hex(crypto:hash(sha256, erlang:term_to_binary({node(), erlang:system_time()}))).
