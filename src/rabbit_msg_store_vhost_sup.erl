-module(rabbit_msg_store_vhost_sup).

-include("rabbit.hrl").

-behaviour(supervisor2).

-export([start_link/4, init/1, add_vhost/2, delete_vhost/2,
         client_init/5, successfully_recovered_state/2]).

%% Internal
-export([start_store_for_vhost/5]).

start_link(Type, MsgStoreModule, VhostsClientRefs, StartupFunState)
        when is_map(VhostsClientRefs); VhostsClientRefs == undefined ->
    supervisor2:start_link({local, Type}, ?MODULE,
                           [Type, MsgStoreModule, VhostsClientRefs, StartupFunState]).

init([Type, MsgStoreModule, VhostsClientRefs, StartupFunState]) ->
    ets:new(Type, [named_table, public]),
    {ok, {{simple_one_for_one, 1, 1},
        [{rabbit_msg_store_vhost, {rabbit_msg_store_vhost_sup, start_store_for_vhost,
                                   [Type, MsgStoreModule, VhostsClientRefs, StartupFunState]},
           transient, infinity, supervisor, [MsgStoreModule]}]}}.


add_vhost(Type, VHost) ->
    {VHostPid, _MsgStoreModule} = maybe_start_store_for_vhost(Type, VHost),
    {ok, VHostPid}.

start_store_for_vhost(Type, MsgStoreModule, VhostsClientRefs, StartupFunState, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid ->
            VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
            ok = rabbit_file:ensure_dir(VHostDir),
            rabbit_log:info("Making sure message store directory '~s' for vhost '~s' exists~n", [VHostDir, VHost]),
            VhostRefs = refs_for_vhost(VHost, VhostsClientRefs),
            VhostStartupFunState = startup_fun_state_for_vhost(StartupFunState, VHost),
            case MsgStoreModule:start_link(Type, VHostDir, VhostRefs, VhostStartupFunState) of
                {ok, Pid} ->
                    ets:insert(Type, {VHost, Pid, MsgStoreModule}),
                    {ok, Pid, MsgStoreModule};
                Other     -> Other
            end;
        {Pid, MsgStoreModule} when is_pid(Pid) ->
            {error, {already_started, Pid, MsgStoreModule}}
    end.

startup_fun_state_for_vhost({Fun, {start, [#resource{}|_] = QNames}}, VHost) ->
    QNamesForVhost = [QName || QName = #resource{virtual_host = VH} <- QNames,
                               VH == VHost ],
    {Fun, {start, QNamesForVhost}};
startup_fun_state_for_vhost(State, _VHost) -> State.

refs_for_vhost(_, undefined) -> undefined;
refs_for_vhost(VHost, Refs) ->
    case maps:find(VHost, Refs) of
        {ok, Val} -> Val;
        error -> []
    end.

delete_vhost(Type, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid               -> ok;
        {Pid, MsgStoreModule} when is_pid(Pid) ->
            supervisor2:terminate_child(Type, Pid),
            cleanup_vhost_store(Type, VHost, Pid, MsgStoreModule)
    end,
    ok.

-spec client_init(atom(), reference(), fun(), fun(), binary()) -> {atom(), term()}.
client_init(Type, Ref, MsgOnDiskFun, CloseFDsFun, VHost) ->
    {VHostPid, MsgStoreModule} = maybe_start_store_for_vhost(Type, VHost),
    {MsgStoreModule,
     MsgStoreModule:client_init(VHostPid, Ref, MsgOnDiskFun, CloseFDsFun)}.

-spec maybe_start_store_for_vhost(atom(), binary()) -> {pid(), atom()}.
maybe_start_store_for_vhost(Type, VHost) ->
    case supervisor2:start_child(Type, [VHost]) of
        {ok, Pid, MsgStoreModule}                       -> {Pid, MsgStoreModule};
        {error, {already_started, Pid, MsgStoreModule}} -> {Pid, MsgStoreModule};
        Error                                           -> throw(Error)
    end.

-spec vhost_store_pid(atom(), binary()) -> no_pid | {pid(), atom()}.
vhost_store_pid(Type, VHost) ->
    case ets:lookup(Type, VHost) of
        []    -> no_pid;
        [{VHost, Pid, MsgStoreModule}] ->
            case erlang:is_process_alive(Pid) of
                true  -> {Pid, MsgStoreModule};
                false ->
                    cleanup_vhost_store(Type, VHost, Pid, MsgStoreModule),
                    no_pid
            end
    end.

cleanup_vhost_store(Type, VHost, Pid, MsgStoreModule) ->
    ets:delete_object(Type, {VHost, Pid, MsgStoreModule}).

successfully_recovered_state(Type, VHost) ->
    case vhost_store_pid(Type, VHost) of
        no_pid               ->
            throw({message_store_not_started, Type, VHost});
        {Pid, MsgStoreModule} when is_pid(Pid) ->
            MsgStoreModule:successfully_recovered_state(Pid)
    end.
