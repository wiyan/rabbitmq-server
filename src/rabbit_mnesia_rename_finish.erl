%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mnesia_rename_finish).
-include("rabbit.hrl").
-compile(export_all).
-export([maybe_finish/1]).

-define(CONVERT_TABLES, [schema, rabbit_durable_queue]).

%% Supports renaming the nodes in the Mnesia database. In order to do
%% this, we take a backup of the database, traverse the backup
%% changing node names and pids as we go, then restore it.
%%
%% That's enough for a standalone node, for clusters the story is more
%% complex. We can take pairs of nodes From and To, but backing up and
%% restoring the database changes schema cookies, so if we just do
%% this on all nodes the cluster will refuse to re-form with
%% "Incompatible schema cookies.". Therefore we do something similar
%% to what we do for upgrades - the first node in the cluster to
%% restart becomes the authority, and other nodes wipe their own
%% Mnesia state and rejoin. They also need to tell Mnesia the old node
%% is not coming back.
%%
%% If we are renaming nodes one at a time then the running cluster
%% might not be aware that a rename has taken place, so after we wipe
%% and rejoin we then update any tables (in practice just
%% rabbit_durable_queue) which should be aware that we have changed.

%%----------------------------------------------------------------------------

-spec maybe_finish([node()]) -> 'ok'.

%%----------------------------------------------------------------------------

maybe_finish(AllNodes) ->
    case rabbit_file:read_term_file(rabbit_mnesia_rename:rename_config_name()) of
        {ok, [{FromNode, ToNode}]} -> finish(FromNode, ToNode, AllNodes);
        _                          -> ok
    end.

finish(FromNode, ToNode, AllNodes) ->
    case node() of
        ToNode ->
            case rabbit_upgrade:nodes_running(AllNodes) of
                [] -> finish_primary(FromNode, ToNode);
                _  -> finish_secondary(FromNode, ToNode, AllNodes)
            end;
        FromNode ->
            rabbit_log:info(
              "Abandoning rename from ~s to ~s since we are still ~s~n",
              [FromNode, ToNode, FromNode]),
            [{ok, _} = file:copy(rabbit_mnesia_rename:backup_of_conf(F), F) || F <- rabbit_mnesia_rename:config_files()],
            ok = rabbit_file:recursive_delete([rabbit_mnesia:dir()]),
            ok = rabbit_file:recursive_copy(
                   rabbit_mnesia_rename:mnesia_copy_dir(), rabbit_mnesia:dir()),
            delete_rename_files();
        _ ->
            %% Boot will almost certainly fail but we might as
            %% well just log this
            rabbit_log:info(
              "Rename attempted from ~s to ~s but we are ~s - ignoring.~n",
              [FromNode, ToNode, node()])
    end.

finish_primary(FromNode, ToNode) ->
    rabbit_log:info("Restarting as primary after rename from ~s to ~s~n",
                    [FromNode, ToNode]),
    delete_rename_files(),
    ok.

finish_secondary(FromNode, ToNode, AllNodes) ->
    rabbit_log:info("Restarting as secondary after rename from ~s to ~s~n",
                    [FromNode, ToNode]),
    rabbit_upgrade:secondary_upgrade(AllNodes),
    rename_in_running_mnesia(FromNode, ToNode),
    delete_rename_files(),
    ok.

delete_rename_files() ->
    ok = rabbit_file:recursive_delete([rabbit_mnesia_rename:dir()]).

mini_map(FromNode, ToNode) -> dict:from_list([{FromNode, ToNode}]).

rename_in_running_mnesia(FromNode, ToNode) ->
    All = rabbit_mnesia:cluster_nodes(all),
    Running = rabbit_mnesia:cluster_nodes(running),
    case {lists:member(FromNode, Running), lists:member(ToNode, All)} of
        {false, true}  -> ok;
        {true,  _}     -> exit({old_node_running,        FromNode});
        {_,     false} -> exit({new_node_not_in_cluster, ToNode})
    end,
    {atomic, ok} = mnesia:del_table_copy(schema, FromNode),
    Map = mini_map(FromNode, ToNode),
    {atomic, _} = transform_table(rabbit_durable_queue, Map),
    ok.

transform_table(Table, Map) ->
    mnesia:sync_transaction(
      fun () ->
              mnesia:lock({table, Table}, write),
              transform_table(Table, Map, mnesia:first(Table))
      end).

transform_table(_Table, _Map, '$end_of_table') ->
    ok;
transform_table(Table, Map, Key) ->
    [Term] = mnesia:read(Table, Key, write),
    ok = mnesia:write(Table, rabbit_mnesia_rename:update_term(Map, Term), write),
    transform_table(Table, Map, mnesia:next(Table, Key)).
