-module(mzb_logger_tcp_protocol).

-behaviour(ranch_protocol).
-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server
-export([init/1,
         init/4,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket, transport}).

start_link(Ref, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Transport, Opts]).

dispatch(close_req, #state{socket = Socket, transport = Transport} = State) ->
    Transport:close(Socket),
    {stop, normal, State};

dispatch(Unhandled, State) ->
    logger:error("Unhandled tcp message: ~p", [Unhandled]),
    {noreply, State}.

init([State]) -> {ok, State}.

init(Ref, Socket, Transport, Opts) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}, {keepalive, true}, binary]),

    ok = case lists:member(user, Opts) of
        true ->
            LogQueueMax = application:get_env(mzbench, log_queue_max_len, undefined),
            LogRateLimit = application:get_env(mzbench, log_rate_limit, undefined),
            gen_event:add_handler(logger_event, {mzb_logger_tcp, Socket}, [info, Socket, LogQueueMax, LogRateLimit, "errors.user"]);
        _ -> ok
    end,
    ok = case lists:member(system, Opts) of
        true -> gen_event:add_handler(logger_system_event, {mzb_logger_tcp, Socket}, [info, Socket, undefined, 0, "errors.system"]);
        _ -> ok
    end,
    gen_server:enter_loop(?MODULE, [], #state{socket=Socket, transport=Transport}).

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info({tcp, Socket, Msg}, State = #state{socket = Socket}) ->
    dispatch(erlang:binary_to_term(Msg), State);

handle_info({tcp_error, _, Reason}, State) ->
    logger:warning("~p was closed with reason: ~p", [?MODULE, Reason]),
    {stop, Reason, State};

handle_info(Info, State) ->
    logger:error("~p has received unexpected info: ~p", [?MODULE, Info]),
    {stop, normal, State}.

handle_cast(Msg, State) ->
    logger:error("~p has received unexpected cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_call(Request, _From, State) ->
    logger:error("~p has received unexpected call: ~p", [?MODULE, Request]),
    {reply, ignore, State}.

terminate(_Reason, #state{socket = Socket}) ->
    gen_event:delete_handler(logger_event, {mzb_logger_tcp, Socket}, []),
    gen_event:delete_handler(logger_system_event, {mzb_logger_tcp, Socket}, []),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

