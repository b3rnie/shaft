%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(shaft_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/1
        , cmd/3
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("shaft/include/shaft.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%%_* Macros ===========================================================
-define(timeout, 5000).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { connection_pid
           , channel_pid
           , subs            = dict:new()
           , pubs            = []
           , seen_pubno      = 0
           , next_pubno      = 1
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, [{timeout, ?timeout}]).

stop(Ref) ->
  gen_server:call(Ref, stop).

cmd(Ref, Cmd, Args) ->
  gen_server:call(Ref, {Cmd, Args}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  case try_connect(assoc(hosts,    Args, [{"localhost", 5672}]),
                   assoc(username, Args, "guest"),
                   assoc(password, Args, "guest"),
                   assoc(vhost,    Args, "virtual_host")) of
    {ok, Pid} ->
      erlang:link(Pid),
      {ok, ChannelPid} = amqp_connection:open_channel(Pid),
      erlang:link(ChannelPid),
      #'confirm.select_ok'{} =
        amqp_channel:call(ChannelPid, #'confirm.select'{}),
      Qos = #'basic.qos'{prefetch_count = 1},
      #'basic.qos_ok'{} = amqp_channel:call(ChannelPid, Qos),
      ok = amqp_channel:register_confirm_handler(ChannelPid, self()),
      {ok, #s{connection_pid=Pid, channel_pid=ChannelPid}};
    {error, Rsn} ->
      {stop, Rsn}
  end.

terminate(_Rsn, S) ->
  ok = amqp_channel:close(S#s.channel_pid),
  amqp_connection:close(S#s.connection_pid),
  ok.

handle_call({subscribe, [Fun, Queue, Options]}, From, S) ->
  case dict:find(Queue, S#s.subs) of
    {ok, {open, _}}  -> {reply, {error, already_subscribed}, S};
    {ok, {setup, _}} -> {reply, {error, setup_in_progress}, S};
    {ok, {close, _}} -> {reply, {error, close_in_progress}, S};
    error ->
      QueueBin = erlang:list_to_binary(Queue),
      Consume = #'basic.consume'{
         queue        = QueueBin
       , consumer_tag = QueueBin
       , no_ack       = assoc(ack,       Options, true)
       , exclusive    = assoc(exclusive, Options, false)
       },
      #'basic.consume_ok'{consumer_tag = QueueBin} =
        amqp_channel:call(S#s.channel_pid, Consume),
      {noreply, S#s{subs = dict:store(Queue, {setup, {Fun, From}}, S#s.subs)}}
  end;

handle_call({unsubscribe, [Queue, _Options]}, From, S) ->
  case dict:find(Queue, S#s.subs) of
    {ok, {setup, _}} -> {reply, {error, setup_in_progress}, S};
    {ok, {close, _}} -> {reply, {error, close_in_progress}, S};
    {ok, {open, _}}  ->
      Cancel = #'basic.cancel'{consumer_tag = erlang:list_to_binary(Queue)},
      #'basic.cancel_ok'{} = amqp_channel:call(S#s.channel_pid, Cancel),
      {noreply, S#s{subs = dict:store(Queue, {close,From}, S#s.subs)}};
    error ->
      {reply, {error, not_subscribed}, S}
  end;

handle_call({publish, [Exchange, RoutingKey, Payload, Options]}, From, S) ->
  Publish = #'basic.publish'{
     exchange    = erlang:list_to_binary(Exchange)
   , routing_key = erlang:list_to_binary(RoutingKey)
   , mandatory   = assoc(mandatory, Options, false)
   , immediate   = assoc(immediate, Options, false)
   },
  Props = #'P_basic'{
     delivery_mode  = case assoc(persistent, Options, false) of
                        false -> 1;
                        true  -> 2
                      end
   , correlation_id = assoc(correlation_id, Options, undefined)
   , message_id     = assoc(message_id,     Options, <<0>>)
   , reply_to       = assoc(reply_to,       Options, undefined)
   },
  Msg = #amqp_msg{ payload = erlang:term_to_binary(Payload)
                 , props   = Props
                 },
  ok = amqp_channel:cast(S#s.channel_pid, Publish, Msg),
  case assoc(async, Options, true) of
    true  -> {reply, ok, S#s{next_pubno=S#s.next_pubno+1}};
    false -> {noreply, S#s{next_pubno=S#s.next_pubno+1,
                           pubs=[{S#s.next_pubno,From} | S#s.pubs]
                          }}
  end;

handle_call({exchange_declare, [Exchange, Options]}, _From, S) ->
  Declare = #'exchange.declare'{
     exchange    = erlang:list_to_binary(Exchange)
   , ticket      = assoc(ticket,      Options, 0)
   , type        = erlang:list_to_binary(assoc(type, Options, "direct"))
   , passive     = assoc(passive,     Options, false)
   , durable     = assoc(durable,     Options, false)
   , auto_delete = assoc(auto_delete, Options, false)
   , internal    = assoc(internal,    Options, false)
   , nowait      = assoc(nowait,      Options, false)
   , arguments   = assoc(arguments,   Options, [])
   },
  #'exchange.declare_ok'{} =
    amqp_channel:call(S#s.channel_pid, Declare),
  {reply, ok, S};

handle_call({exchange_delete, [Exchange, Options]}, _From, S) ->
  Delete = #'exchange.delete'{
     exchange  = erlang:list_to_binary(Exchange)
   , ticket    = assoc(ticket,    Options, 0)
   , if_unused = assoc(if_unused, Options, false)
   , nowait    = assoc(nowait,    Options, false)
   },
  #'exchange.delete_ok'{} = amqp_channel:call(S#s.channel_pid, Delete),
  {reply, ok, S};

handle_call({queue_declare, [Queue0, Options]}, _From, S) ->
  Declare = #'queue.declare'{
     queue       = erlang:list_to_binary(Queue0)
   , ticket      = assoc(ticket,      Options, 0)
   , passive     = assoc(passive,     Options, false)
   , exclusive   = assoc(exclusive,   Options, false)
   , durable     = assoc(durable,     Options, false)
   , auto_delete = assoc(auto_delete, Options, false)
   , nowait      = assoc(nowait,      Options, false)
   , arguments   = assoc(arguments,   Options, [])
   },
  #'queue.declare_ok'{queue = Queue} = %% "" creates a new queue
    amqp_channel:call(S#s.channel_pid, Declare),
  {reply, {ok, erlang:binary_to_list(Queue)}, S};

handle_call({queue_delete, [Queue, Options]}, _From, S) ->
  Delete = #'queue.delete'{
     queue = Queue
   , ticket    = assoc(ticket,    Options, 0)
   , if_unused = assoc(if_unused, Options, false)
   , if_empty  = assoc(if_empty,  Options, false)
   , nowait    = assoc(nowait,    Options, false)
   },
  #'queue.delete_ok'{message_count = _MessageCount} =
    amqp_channel:call(S#s.channel_pid, Delete),
  {reply, ok, S};

handle_call({bind, [Queue, Exchange, RoutingKey]}, _From, S) ->
  Binding = #'queue.bind'{ queue       = erlang:list_to_binary(Queue)
                         , exchange    = erlang:list_to_binary(Exchange)
                         , routing_key = erlang:list_to_binary(RoutingKey)},
  #'queue.bind_ok'{} = amqp_channel:call(S#s.channel_pid, Binding),
  {reply, ok, S};

handle_call({unbind, [Queue, Exchange, RoutingKey]}, _From, S) ->
  Binding = #'queue.unbind'{ queue       = erlang:list_to_binary(Queue)
                           , exchange    = erlang:list_to_binary(Exchange)
                           , routing_key = erlang:list_to_binary(RoutingKey)},
  #'queue.unbind_ok'{} = amqp_channel:call(S#s.channel_pid, Binding),
  {reply, ok, S}.

handle_cast(_, S) ->
  {stop, bad_cast, S}.

%% subscribe response
handle_info(#'basic.consume_ok'{consumer_tag = Queue}, S) ->
  ?info("basic.consume_ok: ~p", [Queue]),
  QueueStr = erlang:binary_to_list(Queue),
  {setup, {Fun, From}} = dict:fetch(QueueStr, S#s.subs),
  gen_server:reply(From, ok),
  {noreply, S#s{subs=dict:store(QueueStr, {open, Fun}, S#s.subs)}};

%% unsubscribe
handle_info(#'basic.cancel_ok'{consumer_tag = Queue}, S) ->
  ?info("basic.cancel_ok: ~p", [Queue]),
  QueueStr = erlang:binary_to_list(Queue),
  {close, From} = dict:fetch(QueueStr, S#s.subs),
  gen_server:reply(From, ok),
  {noreply, S#s{subs=dict:erase(QueueStr, S#s.subs)}};

handle_info({#'basic.deliver'{ consumer_tag = ConsumerTag
                             , delivery_tag = DeliveryTag
                             , exchange     = Exchange
                             , routing_key  = RoutingKey},
             #amqp_msg{ payload = Payload
                      , props   =
                          #'P_basic'{ reply_to       = To
                                    , correlation_id = Id
                                    , message_id     = MsgId}
                      }}, S) ->
  {open, Fun} = dict:fetch(erlang:binary_to_list(ConsumerTag), S#s.subs),
  Fun(erlang:binary_to_term(Payload)),
  Ack = #'basic.ack'{delivery_tag = DeliveryTag},
  amqp_channel:cast(S#s.channel_pid, Ack),
  {noreply, S};

%% unroutable
handle_info({#'basic.return'{ reply_text = <<"unroutable">>
                            , exchange   = Exchange}, Payload}, S) ->
  %% slightly drastic for now.
  {stop, {unroutable, Exchange, Payload}, S};

handle_info(#'basic.ack'{ delivery_tag = Tag
                        , multiple     = Multiple
                        }, S) ->
  Pubs = respond_pubs(ok, Tag, Multiple, S#s.pubs),
  {noreply, S#s{pubs=Pubs}};

handle_info(#'basic.nack'{ delivery_tag = Tag
                         , multiple     = Multiple
                         }, S) ->
  Pubs = respond_pubs({error, nack}, Tag, Multiple, S#s.pubs),
  {noreply, S#s{pubs=Pubs}};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
try_connect([{Host,Port}|Hosts], Username, Password, VHost) ->
  case amqp_connection:start(
         #amqp_params_network{
             username     = erlang:list_to_binary(Username)
           , password     = erlang:list_to_binary(Password)
           , virtual_host = erlang:list_to_binary(VHost)
           , host         = erlang:list_to_binary(Host)
           , port         = Port
           , heartbeat    = 0
           }) of
    {ok, Pid} ->
      {ok, Pid};
    {error, Rsn} ->
      try_connect(Hosts, Username, Password, VHost)
  end;
try_connect([], _Username, _Password, _VHost) ->
  {error, unable_to_connect}.

respond_pubs(Ret, Tag, Multiple, Pubs) ->
  case Multiple of
    false ->
      {value, {Tag, From}, Pubs} = lists:keytake(Tag, 1, Pubs),
      gen_server:reply(From, Ret),
      Pubs;
    true ->
      lists:filter(fun({ Id, _From}) when Id > Tag -> true;
                      ({_Id,  From}) ->
                       gen_server:reply(From, Ret),
                       false
                   end, Pubs)
  end.

assoc(K, L, D) ->
  case lists:keyfind(K, 1, L) of
    {K, V} -> V;
    false  -> D
  end.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
