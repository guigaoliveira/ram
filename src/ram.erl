%% ==========================================================================================================
%% Ram - An in-memory distributed KV store for Erlang and Elixir.
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

%% ===================================================================
%% @doc Exposes all of the Key Value store APIs.
%%
%% Ram doesn't have to run on every node of the Erlang cluster. You may start Ram only on those nodes where you
%% need it. When started, Ram creates a logical overlay network running on top of the Erlang distribution cluster.
%%
%% Nodes where Ram runs form a subcluster: they will synchronize data between themselves, and themselves only.
%% All of the data is replicated on every node of the subcluster.
%%
%% <h2>Quickstart</h2>
%% <h3>Elixir</h3>
%% ```
%% iex(1)> :ram.get("key")
%% :undefined
%% iex(2)> :ram.put("key", "value")
%% :ok
%% iex(3)> :ram.get("key")
%% "value"
%% '''
%% <h3>Erlang</h3>
%% ```
%% 1> ram:get("key").
%% undefined
%% 2> ram:put("key", "value").
%% ok
%% 3> ram:get("key").
%% "value"
%% '''
%%
%% <h2>Internals</h2>
%% Ram operations are <strong>A</strong>tomic (take effect on all nodes involved, or on none of the nodes),
%% <strong>C</strong>onsistent (the data is the same across all nodes)
%% and <strong>I</strong>solated (operations on different nodes in a network do not interfere with each other).
%% They are not <strong>D</strong>urable since Ram is an in-memory only database.
%%
%% To do so, every operation creates a global lock with {@link global:trans/4} in the caller process, which then
%% runs a transaction with a 2-phase commit protocol. If transactions fail, they raise
%% `error({commit_timeout, {bad_nodes, BadNodes}})' where `BadNodes' is the list of subcluster nodes where the
%% transaction could not complete.
%%
%% <h2>Conflict Resolution</h2>
%% In case of net splits or bad network conditions, a specific Key might get put simultaneously on two different nodes.
%% When this happens, the cluster experiences a Key conflict.
%%
%% Ram will resolve this conflict by choosing a single Value. By default, Ram keeps track of the time
%% when a registration takes place with {@link erlang:system_time/0}, compares values between conflicting processes and
%% keeps the one with the higher value (the Value that was put more recently). This is a very simple mechanism
%% that can be imprecise, as system clocks are not perfectly aligned in a cluster.
%% @end
%% ===================================================================
-module(ram).

%% API
-export([start/0, stop/0]).
-export([subcluster_nodes/0]).
-export([get/1, get/2, fetch/1]).
-export([put/2]).
-export([update/3]).
-export([delete/1]).

%% ===================================================================
%% API
%% ===================================================================
%% @doc Starts Ram manually.
%%
%% In most cases Ram will be started as one of your application's dependencies,
%% however you may use this helper method to start it manually.
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(ram),
    ok.

%% @doc Stops Ram manually.
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(ram).

%% @doc Returns the nodes of Ram's subcluster.
-spec subcluster_nodes() -> [node()] | not_running.
subcluster_nodes() ->
    ram_kv:subcluster_nodes().

%% @equiv get(Key, undefined)
%% @end
-spec get(Key :: term()) -> Value :: term().
get(Key) ->
    get(Key, undefined).

%% @doc Returns the Key's Value or Default if the Key is not found.
%%
%% <h2>Examples</h2>
%% <h3>Elixir</h3>
%% ```
%% iex(1)> :ram.get("key")
%% :undefined
%% iex(2)> :ram.get("key", "default")
%% "default"
%% iex(3)> :ram.put("key", "value")
%% :ok
%% iex(4)> :ram.get("key")
%% "value"
%% '''
%% <h3>Erlang</h3>
%% ```
%% 1> ram:get("key").
%% undefined
%% 2> ram:get("key", "default").
%% "default"
%% 3> ram:put("key", "value").
%% ok
%% 4> ram:get("key").
%% "value"
%% '''
-spec get(Key :: term(), Default :: term()) -> Value :: term().
get(Key, Default) ->
    ram_kv:get(Key, Default).

%% @doc Looks up a Key.
%%
%% Returns `error' if the Key is not found.
-spec fetch(Key :: term()) -> {ok, Value :: term()} | error.
fetch(Key) ->
    ram_kv:fetch(Key).

%% @doc Puts a Value for a Key.
-spec put(Key :: term(), Value :: term()) -> ok.
put(Key, Value) ->
    ram_kv:put(Key, Value).

%% @doc Atomically updates a Key with the given function.
%%
%% If Key is found then the existing Value is passed to the fun and its result is used as the updated Value of Key.
%% If Key is not found, Default is put as the Value of Key. The Default value will not be passed through the update function.
%%
%% <h2>Examples</h2>
%% <h3>Elixir</h3>
%% ```
%% iex(1)> update_fun = fn existing_value -> existing_value * 2 end
%% #Function<44.65746770/1 in :erl_eval.expr/5>
%% iex(2)> :ram.update("key", 10, update_fun)
%% ok
%% iex(3)> :ram.get("key")
%% 10
%% iex(4)> :ram.update("key", 10, update_fun)
%% ok
%% iex(5)> :ram.get("key")
%% 20
%% '''
%% <h3>Erlang</h3>
%% ```
%% 1> UpdateFun = fun(ExistingValue) -> ExistingValue * 2 end.
%% #Fun<erl_eval.44.65746770>
%% 2> ram:update("key", 10, UpdateFun).
%% ok
%% 3> ram:get("key").
%% 10
%% 4> ram:update("key", 10, UpdateFun).
%% ok
%% 5> ram:get("key").
%% 20
%% '''
-spec update(Key :: term(), Default :: term(), function()) -> ok.
update(Key, Default, Fun) ->
    ram_kv:update(Key, Default, Fun).

%% @doc Deletes a Key.
-spec delete(Key :: term()) -> ok.
delete(Key) ->
    ram_kv:delete(Key).
