%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(per_user_connection_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, cluster_size_1}
     %% , {group, cluster_size_2}
    ].

groups() ->
    [
      {cluster_size_1, [], [
          most_basic_single_node_connection_count,
          single_node_single_user_connection_count
        ]},
      {cluster_size_2, [], [
        ]},
      {cluster_rename, [], [
        ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% see partitions_SUITE
-define(DELAY, 9000).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [
                                               fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
                                              ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1, Config) ->
    init_per_multinode_group(cluster_size_1, Config, 1);
init_per_group(cluster_size_2, Config) ->
    init_per_multinode_group(cluster_size_2, Config, 2);
init_per_group(cluster_rename, Config) ->
    init_per_multinode_group(cluster_rename, Config, 2).

init_per_multinode_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    case Group of
        cluster_rename ->
            % The broker is managed by {init,end}_per_testcase().
            Config1;
        _ ->
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps())
    end.

end_per_group(cluster_rename, Config) ->
    % The broker is managed by {init,end}_per_testcase().
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    clear_all_connection_tracking_tables(Config),
    Config.

end_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    Config1 = ?config(save_config, Config),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(Testcase, Config) ->
    clear_all_connection_tracking_tables(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

clear_all_connection_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_connection_tracking,
        clear_tracked_connection_tables_for_this_node,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_connection_count(Config) ->
    Username = <<"guest">>,
    ?assertEqual(0, count_connections_from(Config, Username)),
    [Conn] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_from(Config, Username)),
    close_connections([Conn]),
    ?assertEqual(0, count_connections_from(Config, Username)).

single_node_single_user_connection_count(Config) ->
    Username = <<"guest">>,
    ?assertEqual(0, count_connections_from(Config, Username)),

    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_from(Config, Username)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_from(Config, Username)),

    [Conn2] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_from(Config, Username)),

    [Conn3] = open_connections(Config, [0]),
    ?assertEqual(2, count_connections_from(Config, Username)),

    [Conn4] = open_connections(Config, [0]),
    ?assertEqual(3, count_connections_from(Config, Username)),

    kill_connections([Conn4]),
    ?assertEqual(2, count_connections_from(Config, Username)),

    [Conn5] = open_connections(Config, [0]),
    ?assertEqual(3, count_connections_from(Config, Username)),

    close_connections([Conn2, Conn3, Conn5]),
    ?assertEqual(0, count_connections_from(Config, Username)).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_connections(Config, NodesAndUsers) ->
    Conns = lists:map(fun
      ({Node, Username, Password}) ->
          rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node,
            <<"/">>, Username, Password);
      (Node) ->
          rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node)
      end, NodesAndUsers),
    timer:sleep(500),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns),
    timer:sleep(500).

kill_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          (catch exit(Conn, please_terminate))
      end, Conns),
    timer:sleep(500).

count_connections_from(Config, Username) ->
    count_connections_from(Config, Username, 0).
count_connections_from(Config, Username, NodeIndex) ->
    timer:sleep(200),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_connections_from, [Username]).

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    set_vhost_connection_limit(Config, VHost, -1).

set_up_user(Config, Username) ->
    rabbit_ct_broker_helpers:add_user(Config, Username),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, <<"/">>),
    set_user_connection_limit(Config, Username, -1).

set_user_connection_limit(Config, Username, Count) ->
    set_user_connection_limit(Config, 0, Username, Count).

set_user_connection_limit(Config, NodeIndex, Username, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:control_action(
      set_user_limits, Node,
      ["{\"max-connections\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(Username)}]).

set_vhost_connection_limit(Config, VHost, Count) ->
    set_vhost_connection_limit(Config, 0, VHost, Count).

set_vhost_connection_limit(Config, NodeIndex, VHost, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:control_action(
      set_vhost_limits, Node,
      ["{\"max-connections\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(VHost)}]).
