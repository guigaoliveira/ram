%% ==========================================================================================================
%% Ram - An ephemeral distributed KV store for Erlang and Elixir.
%%
%% The MIT License (MIT)
%%
%% Copyright (c) 2015 Roberto Ostinelli <roberto@ostinelli.net> and Neato Robotics, Inc.
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
%% @private
-module(ram_backbone).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% internals
-export([add_table_copy/1]).

%% records
-record(state, {}).

%% includes
-include("ram.hrl").

- if (?OTP_RELEASE >= 23).
-define(ETS_OPTIMIZATIONS, [{decentralized_counters, true}]).
-else.
-define(ETS_OPTIMIZATIONS, []).
-endif.

%% ===================================================================
%% API
%% ===================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Options = [],
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], Options).

%% ===================================================================
%% Callbacks
%% ===================================================================

%% ----------------------------------------------------------------------------------------------------------
%% Init
%% ----------------------------------------------------------------------------------------------------------
-spec init([]) ->
    {ok, #state{}} |
    {ok, #state{}, Timeout :: non_neg_integer()} |
    ignore |
    {stop, Reason :: term()}.
init([]) ->
    %% init db with current node set
    init_mnesia_tables(),
    %% init
    {ok, #state{}}.

%% ----------------------------------------------------------------------------------------------------------
%% Call messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: term(), #state{}) ->
    {reply, Reply :: term(), #state{}} |
    {reply, Reply :: term(), #state{}, Timeout :: non_neg_integer()} |
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), Reply :: term(), #state{}} |
    {stop, Reason :: term(), #state{}}.
handle_call(Request, From, State) ->
    error_logger:warning_msg("RAM[~s] Received from ~p an unknown call message: ~p", [node(), From, Request]),
    {reply, undefined, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_cast(Msg :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), #state{}}.
handle_cast(Msg, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown cast message: ~p", [node(), Msg]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% All non Call / Cast messages
%% ----------------------------------------------------------------------------------------------------------
-spec handle_info(Info :: term(), #state{}) ->
    {noreply, #state{}} |
    {noreply, #state{}, Timeout :: non_neg_integer()} |
    {stop, Reason :: term(), #state{}}.
handle_info(Info, State) ->
    error_logger:warning_msg("RAM[~s] Received an unknown info message: ~p", [node(), Info]),
    {noreply, State}.

%% ----------------------------------------------------------------------------------------------------------
%% Terminate
%% ----------------------------------------------------------------------------------------------------------
-spec terminate(Reason :: term(), #state{}) -> terminated.
terminate(Reason, _State) ->
    error_logger:info_msg("RAM[~s] Terminating with reason: ~p", [node(), Reason]),
    %% return
    terminated.

%% ----------------------------------------------------------------------------------------------------------
%% Convert process state when code is changed.
%% ----------------------------------------------------------------------------------------------------------
-spec code_change(OldVsn :: term(), #state{}, Extra :: term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal
%% ===================================================================
-spec init_mnesia_tables() -> any().
init_mnesia_tables() ->
    Self = self(),
    case global:trans({{?MODULE, init_mnesia_tables}, Self},
        fun() ->
            case pg:get_members(?SCOPE, ram_nodes_with_init_table) of
                [] ->
                    %% first node
                    case create_table() of
                        ok ->
                            ok = pg:join(?SCOPE, ram_nodes_with_init_table, Self),
                            %% TODO: find a better way to ensure that pg is propagated
                            timer:sleep(5000),
                            ok;

                        {error, Reason} ->
                            {error, Reason}
                    end;

                [Pid | _] ->
                    %% later nodes
                    case rpc:call(node(Pid), ?MODULE, add_table_copy, [node()]) of
                        ok ->
                            ok = pg:join(?SCOPE, ram_nodes_with_init_table, Self);

                        Error ->
                            Error
                    end
            end
        end) of
        ok -> wait_table_ready();
        _ -> ok
    end.

-spec create_table() -> ok | {error, Reason :: term()}.
create_table() ->
    case mnesia:create_table(?TABLE, [
        {type, set},
        {ram_copies, [node() | nodes()]},
        {attributes, record_info(fields, ?TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}, {write_concurrency, true}] ++ ?ETS_OPTIMIZATIONS}]}
    ]) of
        {atomic, ok} ->
            error_logger:info_msg("RAM[~s] Table was successfully created", [node()]),
            ok;

        {aborted, {already_exists, ?TABLE}} ->
            error_logger:info_msg("RAM[~s] Table already exists", [node()]),
            ok;

        {aborted, {already_exists, ?TABLE, _Node}} ->
            error_logger:info_msg("RAM[~s] Table already exists", [node()]),
            ok;

        Other ->
            error_logger:error_msg("RAM[~s] Error while creating table: ~p", [node(), Other]),
            {error, Other}
    end.

-spec add_table_copy(RemoteNode :: node()) -> ok | {error, Reason :: term()}.
add_table_copy(RemoteNode) ->
    case mnesia:change_config(extra_db_nodes, [RemoteNode]) of
        {ok, _} ->
            error_logger:info_msg("RAM[~s] Extra node ~s successfully added", [node(), RemoteNode]),
            case mnesia:add_table_copy(?TABLE, RemoteNode, ram_copies) of
                {atomic, ok} ->
                    error_logger:info_msg("RAM[~s] Table copy was successfully added on node ~s", [node(), RemoteNode]),
                    ok;

                {aborted, {already_exists, ?TABLE}} ->
                    error_logger:info_msg("RAM[~s] Table copy already added on node ~s", [node(), RemoteNode]),
                    ok;

                {aborted, {already_exists, ?TABLE, _Node}} ->
                    error_logger:info_msg("RAM[~s] Table copy already added on node ~s", [node(), RemoteNode]),
                    ok;

                {aborted, Reason} ->
                    error_logger:info_msg("RAM[~s] Error while adding table copy on node ~s: ~p", [node(), RemoteNode, Reason]),
                    {error, Reason}
            end;

        {error, Reason} ->
            error_logger:info_msg("RAM[~s] Error while adding extra node ~s: ~p", [node(), RemoteNode, Reason]),
            {error, Reason}
    end.

-spec wait_table_ready() -> ok | {error, Reason :: term()}.
wait_table_ready() ->
    case mnesia:wait_for_tables([?TABLE], 10000) of
        {timeout, [?TABLE]} -> {error, timeout};
        Other -> Other
    end.
