%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_plugin_kafka).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-export([ load/1
        , unload/0
        ]).

%% Hooks functions
-export([ 
%% 		 on_client_authenticate/2
%%         , on_client_check_acl/5
        on_client_connected/4
        , on_client_disconnected/3
%%         , on_client_subscribe/4
%%         , on_client_unsubscribe/4
%%         , on_session_created/3
%%         , on_session_resumed/3
%%         , on_session_terminated/3
%%         , on_session_subscribed/4
%%         , on_session_unsubscribed/4
        , on_message_publish/2
%%         , on_message_deliver/3
%%         , on_message_acked/3
%%         , on_message_dropped/3
        ]).

%% Called when the plugin application start
load(Env) ->
	?LOG(error,"init begin ~n", []),
	ekaf_init([Env]),
%%     emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/2, [Env]),
%%     emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
%%     emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
%%     emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
%%     emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
%%     emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
%%     emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
%%     emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
%%     emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).
%%     emqx:hook('message.deliver', fun ?MODULE:on_message_deliver/3, [Env]),
%%     emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
%%     emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, [Env]).


ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
	EventHost = proplists:get_value(event_host, BrokerValues),
	EventPort = proplists:get_value(event_port, BrokerValues),
	EventPartitionTotal = proplists:get_value(event_partition_total, BrokerValues),
	EventTopic = proplists:get_value(event_topic, BrokerValues),
	
	CustomHost = proplists:get_value(custom_host, BrokerValues),
	CustomPort = proplists:get_value(custom_port, BrokerValues),
	CustomPartitionTotal = proplists:get_value(custom_partition_total, BrokerValues),
	CustomTopic = proplists:get_value(custom_topic, BrokerValues),
	
	OnlineHost = proplists:get_value(online_host, BrokerValues),
	OnlinePort = proplists:get_value(online_port, BrokerValues),
	OnlinePartitionTotal = proplists:get_value(online_partition_total, BrokerValues),
	OnlineTopic = proplists:get_value(online_topic, BrokerValues),
	
    ets:new(kafka_config, [named_table, protected, set, {keypos, 1}]),
	
    ets:insert(kafka_config, {event_host, EventHost}),
    ets:insert(kafka_config, {event_port, EventPort}),
	ets:insert(kafka_config, {event_partition_total, EventPartitionTotal}),
	ets:insert(kafka_config, {event_topic, EventTopic}),
	
	ets:insert(kafka_config, {custom_host, CustomHost}),
    ets:insert(kafka_config, {custom_port, CustomPort}),
	ets:insert(kafka_config, {custom_partition_total, CustomPartitionTotal}),
	ets:insert(kafka_config, {custom_topic, CustomTopic}),
	
	ets:insert(kafka_config, {online_host, OnlineHost}),
    ets:insert(kafka_config, {online_port, OnlinePort}),
	ets:insert(kafka_config, {online_partition_total, OnlinePartitionTotal}),
	ets:insert(kafka_config, {online_topic, OnlineTopic}),
	
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(brod),
	ok = brod:start_client([{EventHost,EventPort}], event_client),
	ok = brod:start_client([{CustomHost,CustomPort}], custom_client),
	ok = brod:start_client([{OnlineHost,OnlinePort}], online_client),
%% 	?LOG(error,"event topic is:~s",[EventTopic]),
	ok = brod:start_producer(event_client, list_to_binary(EventTopic), _ProducerConfig = []),
	ok = brod:start_producer(custom_client, list_to_binary(CustomTopic), _ProducerConfig = []),
	ok = brod:start_producer(online_client, list_to_binary(OnlineTopic), _ProducerConfig = []).

%% on_client_authenticate(Credentials = #{client_id := ClientId, password := Password}, _Env) ->
%%     io:format("Client(~s) authenticate, Password:~p ~n", [ClientId, Password]),
%%     {stop, Credentials#{auth_result => success}}.

%% on_client_check_acl(#{client_id := ClientId}, PubSub, Topic, DefaultACLResult, _Env) ->
%%     ?LOG(info,"Client(~s) authenticate, PubSub:~p, Topic:~p, DefaultACLResult:~p~n",
%%              [ClientId, PubSub, Topic, DefaultACLResult]),
%%     {stop, allow}.

on_client_connected(Client, ConnAck, ConnAttrs, _Env) ->
    ?LOG(error,"Client(~p) connected, connack: ~w, conn_attrs:~p~n", [Client, ConnAck, ConnAttrs]).
%% 	Event = [{action, <<"connected">>},
%%                 {clientid, ClientId},
%%                 {username, Username},
%%                 {result, ConnAck}].
%%     produce_event_kafka_log(Event).

on_client_disconnected(Client, ReasonCode, _Env) ->
    ?LOG(error,"Client(~p) disconnected, reason_code: ~w~n", [Client, ReasonCode]).

%% on_client_subscribe(#{client_id := ClientId}, _Properties, RawTopicFilters, _Env) ->
%%     ?LOG(error,"Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
%%     {ok, RawTopicFilters}.

%% on_client_unsubscribe(#{client_id := ClientId}, _Properties, RawTopicFilters, _Env) ->
%%     io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
%%     {ok, RawTopicFilters}.
%% 
%% on_session_created(#{client_id := ClientId}, SessAttrs, _Env) ->
%%     io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).
%% 
%% on_session_resumed(#{client_id := ClientId}, SessAttrs, _Env) ->
%%     io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).
%% 
%% on_session_subscribed(#{client_id := ClientId}, Topic, SubOpts, _Env) ->
%%     io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).
%% 
%% on_session_unsubscribed(#{client_id := ClientId}, Topic, Opts, _Env) ->
%%     io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]).
%% 
%% on_session_terminated(#{client_id := ClientId}, ReasonCode, _Env) ->
%%     io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
%% 	?LOG(error,"sys Publish ~s~n", [emqx_message:format(Message)]),
	S = binary:split(Message#message.topic, <<$/>>, [global, trim]),
	?LOG(error,"sys Publish ~p split ~p last ~s ~n", [Message,S,lists:last(S)]),
	case lists:last(S) of 
		<<"disconnected">> ->
			?LOG(error,"disconnected msg!"),
			produce_event_kafka_log(disconnected, Message);
		<<"connected">> ->
			?LOG(error,"connected msg!"),
 			produce_event_kafka_log(connected, Message);
		Other ->
			?LOG(error,"other msg:~s!",[Other])
	end,
    {ok, Message};

on_message_publish(Message, _Env) ->
	?LOG(error,"Publish ~p~n", [Message]),
	produce_message_kafka_payload(Message),
    {ok, Message}.

%% on_message_deliver(#{client_id := ClientId}, Message, _Env) ->
%%     io:format("Deliver message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
%%     {ok, Message}.
%% 
%% on_message_acked(#{client_id := ClientId}, Message, _Env) ->
%%     io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
%%     {ok, Message}.

%% on_message_dropped(_By, #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
%%     ok;
%% on_message_dropped(#{node := Node}, Message, _Env) ->
%%     io:format("Message dropped by node ~s: ~s~n", [Node, emqx_message:format(Message)]);
%% on_message_dropped(#{client_id := ClientId}, Message, _Env) ->
%%     io:format("Message dropped by client ~s: ~s~n", [ClientId, emqx_message:format(Message)]).

process_message_topic(Topic)->
	S = binary:split(Topic, <<$/>>, [global, trim]),
	?LOG(error,"split Topic: ~p",[S]),
	Size = array:size(array:from_list(S)),
	if 
		Size>=4->
			case lists:last(S) of
				<<"event">> ->
					{ok, event};
				<<"custom">> ->
					{ok, custom};
				Other ->
					?LOG(error,"unknow topic:~s",[Other]),
					{error,"unknow topic:" ++Other}
			end;
		true->
			?LOG(error,"size error:~s",[integer_to_list(Size)]),
			{error, "size error:"++integer_to_list(Size)}
	end.
	

process_message_payload(Payload)->
	case jsx:is_json(Payload) of
		true ->
			BodyResult = jsx:decode(Payload),
			{ok, proplists:get_value(<<"topic">>,BodyResult),proplists:get_value(<<"action">>,BodyResult)};
		false ->
			{error,"Payload is not a json:"++Payload}
	end.

get_kafka_config(Event, Clientid) ->
	case Event of
		event ->
			[{_, Topic}] = ets:lookup(kafka_config, event_topic),
			[{_, PartitionTotal}] = ets:lookup(kafka_config, event_partition_total),
			Partition = erlang:phash2(Clientid) rem PartitionTotal,
			{ok, list_to_binary(Topic), Partition, event_client};
		custom ->
			[{_, Topic}] = ets:lookup(kafka_config, custom_topic),
			[{_, PartitionTotal}] = ets:lookup(kafka_config, custom_partition_total),
			Partition = erlang:phash2(Clientid) rem PartitionTotal,
			{ok, list_to_binary(Topic), Partition, custom_client};
		Other ->
			?LOG(error,"unknow envent type:~s",[Other]),
			{error,"unknow envent type:"++Other}
	end.
	

produce_message_kafka_payload(Message) ->
	Topic = Message#message.topic, 
	case process_message_topic(Topic) of 
		{ok, Event} ->
			#{username:=Username} = Message#message.headers,
			?LOG(error,"username:~s event:~s",[Username,Event]),
			case process_message_payload(Message#message.payload) of
				{ok, PaloadTopic, Action} ->
					{M, S, _} = Message#message.timestamp,
					KafkaPayload = [
							{clientId , Message#message.from},
							{appId , get_app_id(Username)},
							{recvedAt , timestamp()},
							{from,<<"mqtt">>},
							{msgId , gen_msg_id(Event)},
							{mqttTopic , Topic},
							{topic , PaloadTopic},
							{action , Action},
							{timestamp , M * 1000000 + S}
						],
					case get_kafka_config(Event, Message#message.from) of
						{ok, KafkaTopic, Partition, Client} ->
							?LOG(error,"msg payload: ~s topic:~s partition:~s r:~s",[jsx:encode(KafkaPayload),KafkaTopic,  integer_to_list(Partition),
																			integer_to_list(erlang:phash2(Message#message.from))]),
							ok = brod:produce_sync(Client, KafkaTopic, Partition, <<>>, jsx:encode(KafkaPayload));
						{error, Msg} -> 
							?LOG(error,"get_kafka_config error: ~s",[Msg])
					end;
				{error, Msg} ->
					?LOG(error,"msg kafka body error: ~s",[Msg])
			end;
		{error, Msg} ->
			?LOG(error,"process topic error: ~s",[Msg])
	end,
    ok.

timestamp() ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.

gen_msg_id(connected)->
	list_to_binary("rbc"++string:substr(md5:md5(integer_to_list(timestamp()+random:uniform(1000000))), 8, 20));

gen_msg_id(disconnected)->
	list_to_binary("rbd"++string:substr(md5:md5(integer_to_list(timestamp()+random:uniform(1000000))), 8, 20));

gen_msg_id(custom)->
	list_to_binary("rbt"++string:substr(md5:md5(integer_to_list(timestamp()+random:uniform(1000000))), 8, 20));

gen_msg_id(event)->
	list_to_binary("rbe"++string:substr(md5:md5(integer_to_list(timestamp()+random:uniform(1000000))), 8, 20)).

get_app_id(Username)->
	if is_binary(Username) ->
		    UsernameStr = binary:bin_to_list(Username);
	   is_list(Username) ->
			UsernameStr = Username;
	   true -> 
		    UsernameStr = ""
	end,
	Position = string:chr(UsernameStr, $@),
	case Position of
		0->	
			?LOG(error, "[Auth blacklist] username:~s invalid", [Username]),
			<<"">>;
		_->
			list_to_binary(lists:nth(2,string:tokens(UsernameStr,"@")))
	end.

produce_event_kafka_log(connected, Message) ->
	?LOG(error,"connected produce_event_kafka_log: ~p",[Message]),
	PayloadResult = jsx:decode(Message#message.payload),
	Username = proplists:get_value(<<"username">>, PayloadResult),
	Clientid = proplists:get_value(<<"clientid">>, PayloadResult),
	KafkaPayload = [
					{clientId , Clientid},
					{appId , get_app_id(Username)},
					{recvedAt , timestamp()},
					{msgId , gen_msg_id(connected)},
					{mqttTopic , Message#message.topic},
					{action , <<"device.status.online">>},
					{ipaddress , proplists:get_value(<<"ipaddress">>, PayloadResult)},
					{timestamp , proplists:get_value(<<"ts">>, PayloadResult)},
					{isOnline , true},
					{username , Username}
				],
    [{_, Topic}] = ets:lookup(kafka_config, online_topic),
	[{_, PartitionTotal}] = ets:lookup(kafka_config, online_partition_total),
	Partition = erlang:phash2(Clientid) rem PartitionTotal,
	
	?LOG(error,"connected payload: ~s topic:~s total:~s partition:~s r:~s",[jsx:encode(KafkaPayload),Topic, integer_to_list(PartitionTotal), integer_to_list(Partition),
																			integer_to_list(erlang:phash2(Clientid))]),
	ok = brod:produce_sync(online_client, list_to_binary(Topic), Partition, <<>>, jsx:encode(KafkaPayload)),
    ok;

produce_event_kafka_log(disconnected, Message) ->
	?LOG(error,"disconnected produce_event_kafka_log: ~p",[Message]),
	PayloadResult = jsx:decode(Message#message.payload),
	Username = proplists:get_value(<<"username">>, PayloadResult),
	Clientid = proplists:get_value(<<"clientid">>, PayloadResult),
	KafkaPayload = [
					{clientId , Clientid},
					{appId , get_app_id(Username)},
					{recvedAt , timestamp()},
					{msgId , gen_msg_id(connected)},
					{mqttTopic , Message#message.topic},
					{action , <<"device.status.online">>},
					{ipaddress , proplists:get_value(<<"ipaddress">>, PayloadResult)},
					{timestamp , proplists:get_value(<<"ts">>, PayloadResult)},
					{isOnline , false},
					{username , Username}
				],
	?LOG(error,"disconnected payload: ~s",[jsx:encode(KafkaPayload)]),
	[{_, Topic}] = ets:lookup(kafka_config, online_topic),
	[{_, PartitionTotal}] = ets:lookup(kafka_config, online_partition_total),
	Partition = erlang:phash2(Clientid) rem PartitionTotal,
	
	?LOG(error,"disconnected payload: ~s topic:~s total:~s partition:~s r:~s",[jsx:encode(KafkaPayload),Topic, integer_to_list(PartitionTotal), integer_to_list(Partition),
																			integer_to_list(erlang:phash2(Clientid))]),
	ok = brod:produce_sync(online_client, list_to_binary(Topic), Partition, <<>>, jsx:encode(KafkaPayload)),
    ok.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.authenticate', fun ?MODULE:on_client_authenticate/2),
    emqx:unhook('client.check_acl', fun ?MODULE:on_client_check_acl/5),
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.deliver', fun ?MODULE:on_message_deliver/3),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3),
    emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3).

