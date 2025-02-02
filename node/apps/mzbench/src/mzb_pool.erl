-module(mzb_pool).

-export([start_link/4,
         stop/1
        ]).

-behaviour(gen_server).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-include_lib("mzbench_language/include/mzbl_types.hrl").

-define(POLLVARS_INTERVAL_MS, 1000).

-record(s, {
    workers  = [] :: ets:tid(),
    succeed  = 0 :: non_neg_integer(),
    failed   = 0 :: non_neg_integer(),
    stopped  = 0 :: non_neg_integer(),
    name     = undefined :: string(),
    worker_starter = undefined :: undefined | {pid(), reference()},
    current_pool_size = 0 :: non_neg_integer(),
    worker_starter_fun = undefined :: fun(),
    workers_num_fun = undefined :: fun(),
    poll_vars_timer = undefined :: reference()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Pool, Env, NumNodes, Offset) ->
    gen_server:start_link(?MODULE, [Pool, Env, NumNodes, Offset], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Env, NumNodes, Offset]) ->
    Tid = ets:new(pool_workers, [public, {keypos, 1}]),
    _ = random:seed(now()),
    State = #s{workers = Tid},
    {ok, start_workers(Pool, Env, NumNodes, Offset, State)}.

handle_call(stop, _From, #s{workers = Tid, name = Name} = State) ->
    logger:info("[ ~p ] Received stop signal", [Name]),
    ets:foldl(
        fun ({Pid, Ref}, Acc) ->
            erlang:demonitor(Ref, [flush]),
            erlang:exit(Pid, kill),
            Acc
        end, [], Tid),
    ets:delete_all_objects(Tid),
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    logger:error("Unhandled call: ~p", [Req]),
    {stop, {unhandled_call, Req}, State}.

handle_cast({start_worker, WorkerScript, Env, Worker, Node, WId}, #s{workers = Tid, name = PoolName} = State) ->
    Self = self(),
    {P, Ref} = erlang:spawn_monitor(fun() ->
            mzb_worker_runner:run_worker_script(WorkerScript, Env, Worker, Self, PoolName)
        end),
    ets:insert(Tid, {P, Ref}),
    if
        WId < 4 -> logger:info("Starting worker on ~p no ~p", [Node, WId]);
        WId =:= 4 -> logger:info("Starting remaining workers...", []);
        true -> ok
    end,
    {noreply, State};

handle_cast({run_command, PoolNum, Percent, AST}, #s{name = PoolName, workers = Tid} = State) ->
    DestPool = "pool" ++ integer_to_list(PoolNum),
    _ = if PoolName == DestPool ->
            _ = ets:foldl(
                fun ({Pid, _Ref}, Acc) ->
                    RandomValue = random:uniform(),
                    _ = if RandomValue < Percent / 100 ->
                            Pid ! {run_command, AST};
                        true -> ok end,
                    Acc
                end, [], Tid);
        true -> ok end,
    {noreply, State};

handle_cast(Msg, State) ->
    logger:error("Unhandled cast: ~p", [Msg]),
    {stop, {unhandled_cast, Msg}, State}.

handle_info(poll_vars, #s{name = Name,
                          worker_starter = undefined,
                          current_pool_size = CurrentPoolSize,
                          worker_starter_fun = WorkerStartFun,
                          workers_num_fun = WorkersNumFun,
                          workers = Tid} = State) ->
    {_, WorkerNumber, _} = WorkersNumFun(),

    NewState =
        if
            WorkerNumber > CurrentPoolSize ->
                logger:info("[ ~p ] Increasing the number of workers at ~p ~b -> ~b", [Name, node(), CurrentPoolSize, WorkerNumber]),
                WorkerStarter =
                    erlang:spawn_monitor(fun () ->
                        WorkerStartFun(WorkerNumber - CurrentPoolSize, CurrentPoolSize)
                    end),
                State#s{current_pool_size = WorkerNumber, worker_starter = WorkerStarter};
            WorkerNumber < CurrentPoolSize ->
                logger:info("[ ~p ] Decreasing the number of workers at ~p ~b -> ~b", [Name, node(), CurrentPoolSize, WorkerNumber]),
                kill_some_workers(CurrentPoolSize - WorkerNumber, Tid),
                State#s{current_pool_size = WorkerNumber};
            true ->
                State
        end,

    {noreply, restart_pollvars_timer(NewState)};

handle_info(poll_vars, #s{} = State) ->
    {noreply, restart_pollvars_timer(State)};

handle_info({'DOWN', Ref, _, _, normal}, #s{worker_starter = {_, Ref}} = State) ->
    maybe_stop(State#s{worker_starter = undefined});

handle_info({'DOWN', Ref, _, _, Reason}, #s{worker_starter = {_, Ref}} = State) ->
    logger:error("Worker starter has crashed with the reason: ~p", [Reason]),
    {stop, Reason, State};

handle_info({worker_result, _Pid, {ok, _}}, #s{} = State) ->
    {noreply, State#s{succeed = State#s.succeed + 1}};

handle_info({worker_result, Pid, Res}, #s{} = State) ->
    maybe_report_error(Pid, Res),
    {noreply, State#s{failed = State#s.failed + 1}};

handle_info({'DOWN', _Ref, _, Pid, normal}, #s{workers = Workers} = State) ->
    ets:delete(Workers, Pid),
    maybe_stop(State);

handle_info({'DOWN', _Ref, _, Pid, killed_by_pool}, #s{workers = Workers, name = Name} = State) ->
    NewState = State#s{stopped = State#s.stopped + 1},
    case ets:lookup(Workers, Pid) of
        [{Pid, Ref}] ->
            ok = mzb_metrics:notify(mzb_string:format("workers.~s.ended", [Name]), 1),
            ets:delete(Workers, Pid),
            erlang:demonitor(Ref, [flush]);
        _ ->
            logger:error("[ ~p ] Received DOWN from unknown process: ~p", [Name, Pid])
    end,
    maybe_stop(NewState);

handle_info({'DOWN', _Ref, _, Pid, Reason}, #s{workers = Workers, name = Name} = State) ->
    NewState = State#s{failed = State#s.failed + 1},
    case ets:lookup(Workers, Pid) of
        [{Pid, Ref}] ->
            logger:error("[ ~p ] Received DOWN from worker ~p with reason ~p", [Name, Pid, Reason]),
            ets:delete(Workers, Pid),
            erlang:demonitor(Ref, [flush]);
        _ ->
            logger:error("[ ~p ] Received DOWN from unknown process: ~p / ~p", [Name, Pid, Reason])
    end,
    maybe_stop(NewState);

handle_info(Info, State) ->
    logger:error("Unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_workers(Pool, Env, NumNodes, Offset, #s{} = State) ->
    WorkersNumFun = fun () -> eval_worker_number(Pool, Env, NumNodes, Offset) end,
    {Script, WorkerNumber, StartDelay} = WorkersNumFun(),
    #operation{name = pool, args = [PoolOpts|_], meta = Meta} = Pool,
    Name = proplists:get_value(pool_name, Meta),

    Worker = mzbl_script:extract_worker(PoolOpts),

    load_worker(Worker),

    Self = self(),
    Node = node(),
    WorkerStartFun = fun (WNum, StartingFrom) ->
        Numbers = lists:seq(0, WNum - 1),
        StartTime = msnow(),
        lists:foreach(fun(N) ->
                        WId = (StartingFrom + N) * NumNodes + Offset,
                        worker_start_delay(StartDelay, NumNodes, WId, StartTime),
                        WorkerScript = mzbl_ast:add_meta(Script, [{worker_id, WId}]),
                        gen_server:cast(Self, {start_worker, WorkerScript, [{"worker_id", WId}|Env], Worker, Node, WId })
                    end, Numbers)
    end,

    WorkerStarter =
        erlang:spawn_monitor(fun () -> WorkerStartFun(WorkerNumber, 0) end),

    restart_pollvars_timer(
        State#s{name = Name,
                worker_starter = WorkerStarter,
                current_pool_size = WorkerNumber,
                worker_starter_fun = WorkerStartFun,
                workers_num_fun = WorkersNumFun}).

restart_pollvars_timer(#s{poll_vars_timer = OldTimer} = State) ->
    _ = (catch erlang:cancel_timer(OldTimer)),
    PollVarsTimer = erlang:send_after(?POLLVARS_INTERVAL_MS, self(), poll_vars),
    State#s{poll_vars_timer = PollVarsTimer}.

eval_worker_number(Pool, Env, NumNodes, Offset) ->
    #operation{name = pool, args = [PoolOpts, Script]} = Pool,
    Eval = fun (E) -> mzbl_interpreter:eval_std(E, Env) end,
    [Size] = Eval(mzbl_ast:find_operation_and_extract_args(size, PoolOpts, [undefined])),
    [PerNode] = Eval(mzbl_ast:find_operation_and_extract_args(per_node, PoolOpts, [undefined])),
    StartDelay = case mzbl_ast:find_operation_and_extract_args(worker_start, PoolOpts, [undefined]) of
            [undefined] -> undefined;
            [#operation{args = SArgs} = Op] -> Op#operation{args = Eval(SArgs)}
        end,
    Size2 = case [mzb_utility:to_integer_with_default(Size, undefined),
                  mzb_utility:to_integer_with_default(PerNode, undefined)] of
                [undefined, undefined] -> 1;
                [undefined, PN] -> PN * NumNodes;
                [S, undefined] -> S;
                [S, PN] when PN * Offset > S -> 0;
                [S, PN] when NumNodes * PN >= S -> S;
                [S, PN] ->
                    logger:error("Need more nodes, required = ~p, actual = ~p",
                        [mzb_utility:int_ceil(S/PN), NumNodes]),
                    erlang:error({not_enough_nodes})
            end,

    Script2 = mzbl_ast:add_meta(Script, [{pool_size, Size2}, {nodes_num, NumNodes}]),
    {Script2, mzb_utility:int_ceil((Size2 - Offset + 1)/NumNodes), StartDelay}.

kill_some_workers(N, Tid) ->
    Pids = fun F (0, _, Acc) -> lists:reverse(Acc);
               F (_, '$end_of_table', Acc) -> lists:reverse(Acc);
               F (K, Pid, Acc) -> F(K - 1, ets:next(Tid, Pid), [Pid | Acc])
           end (N, ets:first(Tid), []),
    [exit(P, killed_by_pool) || P <- Pids].

load_worker({WorkerProvider, Worker}) ->
    case erlang:apply(WorkerProvider, load, [Worker]) of
        ok -> ok;
        {error, Reason} ->
            logger:error("Worker ~p load failed with reason: ~p", [Worker, Reason]),
            erlang:error({application_start_failed, Worker, Reason})
    end.

maybe_stop(#s{workers = Workers, name = Name, worker_starter = undefined} = State) ->
    case ets:first(Workers) == '$end_of_table' of
        true ->
            logger:info("[ ~p ] All workers have finished", [Name]),
            Info = [{succeed_workers, State#s.succeed},
                    {failed_workers,  State#s.failed},
                    {stopped_workers, State#s.stopped}],
            mzb_interconnect:cast_director({pool_report, self(), Info, true}),
            {stop, normal, State};
        false ->
            {noreply, State}
    end;
maybe_stop(#s{} = State) ->
    {noreply, State}.

maybe_report_error(_, {ok, _}) -> ok;
maybe_report_error(Pid, {error, Reason}) ->
    logger:error("Worker ~p has finished abnormally: ~p", [Pid, Reason]);
maybe_report_error(Pid, {exception, Node, {_C, E, ST}, WorkerState}) ->
    logger:error("Worker ~p on ~p has crashed: ~p~nStacktrace: ~p~nState of worker: ~p", [Pid, Node, E, ST, WorkerState]).

sleep_off(StartTime, ShouldBe) ->
    Current = msnow() - StartTime,
    Sleep = max(0, ShouldBe - Current),
    timer:sleep(Sleep).

worker_start_delay(undefined, _, _, _) -> ok;
worker_start_delay(#operation{name = poisson, args = [Rate]}, Factor, _, _) ->
    % The time between each pair of consecutive events has an exponential
    % distribution with parameter λ and each of these inter-arrival times
    % is assumed to be independent of other inter-arrival times.
    % (http://en.wikipedia.org/wiki/Poisson_process)
    #constant{value = Lambda, units = rps} = mzbl_literals:convert(Rate),
    SleepTime = -(1000*Factor*math:log(random:uniform()))/Lambda,
    timer:sleep(erlang:round(SleepTime));
worker_start_delay(#operation{name = linear, args = [Rate]}, _, WId, StartTime) ->
    #constant{value = RPS, units = rps} = mzbl_literals:convert(Rate),
    sleep_off(StartTime, trunc((WId * 1000) / RPS));
worker_start_delay(#operation{name = pow, args = [Y, W, Time]}, _, WId, StartTime) ->
    #constant{value = T, units = ms} = mzbl_literals:convert(Time),
    sleep_off(StartTime, erlang:round(T*(math:pow(WId/W, 1/Y))));
worker_start_delay(#operation{name = exp, args = [_, _]}, _, 0, _) -> ok;
worker_start_delay(#operation{name = exp, args = [X, Time]}, _, WId, StartTime) ->
    #constant{value = T, units = ms} = mzbl_literals:convert(Time),
    sleep_off(StartTime, erlang:round(T*(math:log((WId+1)/X) + 1))).

msnow() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.
