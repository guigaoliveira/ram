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
-module(ram_SUITE).

%% callbacks
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([groups/0, init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    three_nodes_discover/1
]).

%% include
-include_lib("common_test/include/ct.hrl").
-include("../src/ram.hrl").

%% ===================================================================
%% Callbacks
%% ===================================================================

%% -------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = any()
%% -------------------------------------------------------------------
all() ->
    [
        {group, three_nodes}
    ].

%% -------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName =  atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%			   repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% -------------------------------------------------------------------
groups() ->
    [
        {three_nodes, [shuffle], [
            three_nodes_discover
        ]}
    ].
%% -------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%				Config1 | {skip,Reason} |
%%              {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    Config.

%% -------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% -------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%% -------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%				Config1 | {skip,Reason} |
%%              {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_group(three_nodes, Config) ->
    case ram_test_suite_helper:init_cluster(3) of
        {error_initializing_cluster, Other} ->
            end_per_group(three_nodes, Config),
            {skip, Other};

        NodesConfig ->
            NodesConfig ++ Config
    end;

init_per_group(_GroupName, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%				void() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% -------------------------------------------------------------------
end_per_group(three_nodes, Config) ->
    ram_test_suite_helper:end_cluster(3, Config);
end_per_group(_GroupName, _Config) ->
    ram_test_suite_helper:clean_after_test().

%% -------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%				Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test: ~p", [TestCase]),
    Config.

%% -------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%				void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = any()
%% -------------------------------------------------------------------
end_per_testcase(_, _Config) ->
    ram_test_suite_helper:clean_after_test().

%% ===================================================================
%% Tests
%% ===================================================================

three_nodes_discover(Config) ->
    %% get slaves
    SlaveNode1 = proplists:get_value(ram_slave_1, Config),
    SlaveNode2 = proplists:get_value(ram_slave_2, Config),

    %% start ram
    ok = ram:start(),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), []),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, undefined),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, undefined),

    %% start ram on 1
    ok = rpc:call(SlaveNode1, ram, start, []),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, undefined),

    %% start ram on 2
    ok = rpc:call(SlaveNode2, ram, start, []),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% disconnect node 1 from 2
    rpc:call(SlaveNode1, ram_test_suite_helper, disconnect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node()]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node()]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]),

    %% disconnect master from 1
    ram_test_suite_helper:disconnect_node(SlaveNode1),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node()]),
    %% NB: can't check 1 since disconnected

    %% reconnect all
    ram_test_suite_helper:connect_node(SlaveNode1),
    rpc:call(SlaveNode1, ram_test_suite_helper, connect_node, [SlaveNode2]),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% crash a kv process on 2
    rpc:call(SlaveNode2, ram_test_suite_helper, kill_process, [ram_kv]),
    rpc:call(SlaveNode2, ram_test_suite_helper, wait_process_name_ready, [ram_kv]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node(), SlaveNode1]),

    %% kill ram on 1
    rpc:call(SlaveNode1, ram, stop, []),
    ram_test_suite_helper:assert_cluster(node(), [SlaveNode1, SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode1, [node(), SlaveNode2]),
    ram_test_suite_helper:assert_cluster(SlaveNode2, [node(), SlaveNode1]),

    %% check
    ram_test_suite_helper:assert_subcluster(node(), [SlaveNode2]),
    ram_test_suite_helper:assert_subcluster(SlaveNode1, undefined),
    ram_test_suite_helper:assert_subcluster(SlaveNode2, [node()]).
