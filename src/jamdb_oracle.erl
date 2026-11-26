-module(jamdb_oracle).
-vsn("0.5.12").
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("jamdb_oracle.hrl").

%% API
-export([start_link/1, start/1]).
-export([stop/1]).
-export([sql_query/2, sql_query/3]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

-define(ATTEMPTS, 3).
-define(DEFAULT_TIMEOUT, 2000).

-record(gs_state, {
    cfg :: proplists:proplist(),
    state :: #oraclient{}
}).

%% API
-spec start_link(jamdb_oracle_conn:options()) -> {ok, pid()} | {error, term()}.
start_link(Opts) when is_list(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec start(jamdb_oracle_conn:options()) -> {ok, pid()} | {error, term()}.
start(Opts) when is_list(Opts) ->
    gen_server:start(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop).

sql_query(Pid, Query, Tout) ->
    gen_server:call(Pid, {sql_query, Query, Tout}, Tout).

sql_query(Pid, Query) ->
    gen_server:call(Pid, {sql_query, Query, ?DEFAULT_TIMEOUT}).

%% gen_server callbacks
init(Cfg) ->
    case jamdb_oracle_conn:connect(Cfg) of
        {ok, State} ->
            {ok, #gs_state{state = State, cfg = Cfg}};
        {error, Result, _State} ->
            {stop, Result};
        {error, Type, Result, _State} ->
            {stop, {Type, Result}}
    end.

%% Error types: socket, remote, local
handle_call({sql_query, Query, Tout}, _From, #gs_state{cfg = Cfg, state = State} = GSState) ->
    try jamdb_oracle_conn:sql_query(State, Query, Tout) of
        {ok, Result, State0} ->
            {reply, {ok, Result}, GSState#gs_state{state = State0}};
        {error, socket, _Reason, State0} ->
            jamdb_oracle_conn:disconnect(State0),
            case connect(Cfg, ?ATTEMPTS, State) of
                {ok, State1} ->
                    {reply, {error, socket, close}, GSState#gs_state{state = State1}};
                {error, Result, State1} ->
                    ?LOG_ERROR("Database connection was close - [reason: ~p]", [Result]),
                    {stop, normal, GSState#gs_state{state = State1}};
                {error, Type, Result, State1} ->
                    ?LOG_ERROR("Database connection was close - [error type: ~p] - [reason: ~p]", [Type, Result]),
                    {stop, normal, GSState#gs_state{state = State1}}
            end;
        {error, Type, Reason, State0} ->
            {reply, {error, Type, Reason}, GSState#gs_state{state = State0}}
    catch
        error:_Reason ->
            {stop, normal, State}
    end;
handle_call(stop, _From, #gs_state{state = State} = GSState) ->
    try jamdb_oracle_conn:disconnect(State, 100) of
        {ok, _Result} ->
            {stop, normal, ok, GSState}
    catch
        error:_Reason ->
            {stop, normal, GSState}
    end;
handle_call(_Request, _From, GSState) ->
    {reply, ok, GSState}.

handle_cast(_Msg, GSState) ->
    {noreply, GSState}.

handle_info(timeout, GSState) ->
    {stop, normal, GSState};
handle_info(_Info, GSState) ->
    {noreply, GSState}.

terminate(_Reason, _GSState) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec connect(proplists:proplist(), integer(), #oraclient{}) -> tuple().
connect(_Cfg, 0, State) ->
    ?LOG_ERROR("Database connection attempts was reached"),
    {error, socket, closed, State};
connect(Cfg, Attempts, _State) ->
    case jamdb_oracle_conn:connect(Cfg) of
        {ok, State0} ->
            {ok, State0};
        {error, Result, State0} ->
            {error, Result, State0};
        {error, socket, _Result, State0} ->
            timer:sleep(5000),
            connect(Cfg, Attempts - 1, State0);
        {error, Type, Result, State0} ->
            {error, Type, Result, State0}
    end.