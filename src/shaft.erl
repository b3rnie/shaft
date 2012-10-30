%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(shaft).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/1
        , subscribe/3
        , subscribe/4
        , unsubscribe/2
        , unsubscribe/3
        , publish/4
        , publish/5
        , exchange_declare/2
        , exchange_declare/3
        , exchange_delete/2
        , exchange_delete/3
        , queue_declare/2
        , queue_declare/3
        , queue_delete/2
        , queue_delete/3
        , bind/4
        , unbind/4
        ]).
%%%_* Includes =========================================================
-include_lib("shaft/include/shaft.hrl").

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  shaft_server:start_link(Args).

stop(Ref) ->
  shaft_server:stop(Ref).

%% @spec subscribe(Ref, Fun, Queue) -> ok | {error, Rsn}
%% @doc Subscribe to a queue
subscribe(Ref, Fun, Queue) ->
  subscribe(Ref, Fun, Queue, []).

subscribe(Ref, Fun, Queue, Options) ->
  shaft_server:cmd(Ref, subscribe, [Fun, Queue, Options]).

%% @spec unsubscribe(Ref, Queue) -> ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Ref, Queue) ->
  unsubscribe(Ref, Queue, []).

unsubscribe(Ref, Queue, Options) ->
  shaft_server:cmd(Ref, unsubscribe, [Queue, Options]).

%% @spec publish(Ref, Exchange, RoutingKey, Payload) -> ok
%% @doc Publish a message
publish(Ref, Exchange, RoutingKey, Payload) ->
  publish(Exchange, RoutingKey, Payload, []).

publish(Ref, Exchange, RoutingKey, Payload, Options) ->
  shaft_server:cmd(
    Ref, publish, [Exchange, RoutingKey, Payload, Options]).

%% @spec exchange_declare(Ref, Exchange) -> ok
%% @doc declare a new exchange
exchange_declare(Ref, Exchange) ->
  exchange_declare(Ref, Exchange, []).

exchange_declare(Ref, Exchange, Options) ->
  shaft_server:cmd(Ref, exchange_declare, [Exchange, Options]).

%% @spec exchange_delete(Ref, Exchange) -> ok
%% @doc delete an exchange
exchange_delete(Ref, Exchange) ->
  exchange_delete(Ref, Exchange, []).

exchange_delete(Ref, Exchange, Options) ->
  shaft_server:cmd(Ref, exchange_delete, [Exchange, Options]).

%% @spec queue_declare(Ref, Queue) -> {ok, Queue}
%% @doc declare a new queue
queue_declare(Ref, Queue) ->
  queue_declare(Ref, Queue, []).

queue_declare(Ref, Queue, Options) ->
  shaft_server:cmd(Ref, queue_declare, [Queue, Options]).

%% @spec queue_delete(Ref, Queue) -> ok
%% @doc delete a queue
queue_delete(Ref, Queue) ->
  queue_delete(Queue, []).

queue_delete(Ref, Queue, Options) ->
  shaft_server:cmd(Ref, queue_delete, [Queue, Options]).

%% @spec bind(Ref, Queue, Exchange, RoutingKey) -> ok
%% @doc create routing rule
bind(Ref, Queue, Exchange, RoutingKey) ->
  shaft_server:cmd(Ref, bind, [Queue, Exchange, RoutingKey]).

%% @spec unbind(Ref, Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc remove a routing rule
unbind(Ref, Queue, Exchange, RoutingKey) ->
  shaft_server:cmd(Ref, unbind, [Queue, Exchange, RoutingKey]).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(q, "test_queue").
-define(x, "test_exchange").
-define(r, "test_routing_key").

basic_test() ->
  {ok, Pid} = get_shaft(),
  ok        = shaft:exchange_declare(Pid, ?x),
  {ok, ?q}  = shaft:queue_declare(?q),
  ok        = shaft:bind(?q, ?x, ?r),
  Daddy     = self(),
  ok = shaft:subscribe(Pid, fun(Term) -> {Daddy, Term} end, ?q),
  Expected = lists:map(fun(_) ->
                           Term = random:uniform(),
                           ok = shaft:publish(Pid, ?x, ?r, Term),
                           Term
                       end, lists:seq(1, 100)),
  lists:foreach(fun(Term) ->
                    receive {Daddy, Term} -> ok end
                end, Expected),
  ok = shaft:unbind(Pid, ?q, ?x, ?r),
  ok = shaft:queue_delete(?q),
  ok = shaft:exchange_delete(?x),
  ok.

get_shaft() ->
  shaft:start_link([{username, "guest"},
                    {password, "guest"},
                    {virtual_host, "/"},
                    {hosts, {"localhost", 5672}}]).

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
