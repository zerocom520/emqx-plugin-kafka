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
-export([produce_online_kafka_log/4]).
-export([produce_message_kafka_payload/1]).
-define(config(Key, Config, Default), proplists:get_value(Key, Config, Default)).
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
	
	RequiredAcks = proplists:get_value(required_acks, BrokerValues),
	MaxBatchSize = proplists:get_value(max_batch_size, BrokerValues),
	MaxLingerMs = proplists:get_value(max_linger_ms, BrokerValues),

	ets:new(kafka_config, [named_table, protected, set, {keypos, 1}, {read_concurrency, true}]),
	
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

	ProducerConfig = [{required_acks, RequiredAcks}, {max_batch_size, MaxBatchSize}, {max_linger_ms, MaxLingerMs}],

	?LOG(info, "[Kafka] init config ack:~p batch_size:~p max_linger:~p", [?config(required_acks, ProducerConfig, -1), ?config(max_batch_size,ProducerConfig, -1), ?config(max_linger_ms, ProducerConfig, -1)]),

    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(brod),
	ClientConfig = [{reconnect_cool_down_seconds, 10}, {query_api_versions,false}, {auto_start_producers, true}, {default_producer_config, ProducerConfig}],
	ok = brod:start_client([{EventHost, EventPort}], event_client, ClientConfig),
	ok = brod:start_client([{OnlineHost, OnlinePort}], online_client, ClientConfig),
	ok = brod:start_client([{CustomHost, CustomPort}], custom_client, ClientConfig).
%% 	ok = brod:start_producer(event_client, list_to_binary(EventTopic), ProducerConfig),
%% 	ok = brod:start_producer(online_client, list_to_binary(OnlineTopic), ProducerConfig),
%% 	ok = brod:start_producer(custom_client, list_to_binary(CustomTopic), ProducerConfig).

%% ensure_brod_producer(Client, Topic) ->

on_client_connected(_Client = #{username:=Username, client_id:=Clientid, peername:= Peername, auth_result:= AuthResult}, _ConnAck, _ConnAttrs, _Env) ->
	?LOG(info, "[Kafka] on_client_connected node:~s", [node()]),
	case AuthResult of 
		success ->
			proc_lib:spawn(?MODULE, produce_online_kafka_log, [Clientid, Username, Peername, connected]);
		_Other ->
			?LOG(info, "[Kafka] on_client_connected auth error:~p", [AuthResult])
	end,
	ok;

on_client_connected(_Client = #{username:=_Username, client_id:=_Clientid, peername:= _Peername}, _ConnAck, _ConnAttrs, _Env) ->
	?LOG(info, "[Kafka] on_client_connected node:~s no auth result!", [node()]),
	ok.

on_client_disconnected(_Client = #{username:=Username, client_id:=Clientid, peername:= Peername}, ReasonCode, _Env) ->
	?LOG(info, "[Kafka] on_client_disconnected ResonCode:~p", [ReasonCode]),
	proc_lib:spawn(?MODULE, produce_online_kafka_log, [Clientid, Username, Peername, disconnected]),
	ok.


%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
	proc_lib:spawn(?MODULE, produce_message_kafka_payload, [Message]),
    {ok, Message}.

get_temp_topic(S)->
	case lists:last(S) of
		<<"event">> ->
			<<"">>;
		<<"custom">> ->
			<<"">>;
		Other ->
			Other
	end.

process_message_topic(Topic)->
	S = binary:split(Topic, <<$/>>, [global, trim]),
	Size = array:size(array:from_list(S)),
	if 
		Size>=5 ->
			case lists:nth(5, S) of
				<<"event">> ->
					{ok, event, get_temp_topic(S)};
				<<"custom">> ->
					{ok, custom, get_temp_topic(S)};
				_Other ->
					{error, "unknow topic:" ++ Topic}
			end;
		true->
			{error, "topic size error:" ++ integer_to_list(Size)}
	end.
	

get_proplist_value(Key, Proplist, DefaultValue)->
	case proplists:get_value(Key, Proplist) of
		undefined ->
			DefaultValue;
		Other ->
			Other
	end.

process_message_payload(Payload, TempTopic)->
	ValidPayload = get_valid_payload(Payload),
	case jsx:is_json(ValidPayload) of
		true ->
			BodyResult = jsx:decode(ValidPayload),
			Topic = get_proplist_value(<<"topic">>, BodyResult, <<"">>),
			Action = get_proplist_value(<<"action">>, BodyResult, TempTopic),
			DataResult = proplists:delete(<<"action">>, proplists:delete(<<"topic">>, proplists:delete(<<"timestamp">>, BodyResult))),
			{ok, Topic, Action, DataResult};
		false ->
			{error, "Payload is not a json"}
	end.

get_valid_payload(Payload) ->
	Last = binary:last(Payload),
	case Last of
		0 ->
			ValidPayload = lists:nth(1, binary:split(Payload, [<<0>>])),
			?LOG(debug, "[kafka] error payload hava null str, valid:~p", [ValidPayload]),
			ValidPayload;
		_Other ->
			Payload
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
			{error, "unknow envent type:" ++ Other}
	end.
	
produce_message_kafka_payload(Message = #message{headers = #{peername:= Peername, username:=Username}, from = Clientid, topic = Topic}) ->
	{_Ip, IpPort} = get_ip_str(Peername),
	LoggerHeader = list_to_binary([Clientid, ":" , IpPort]),
	case process_message_topic(Topic) of 
		{ok, Event, TempTopic} ->
			case process_message_payload(Message#message.payload, TempTopic) of
				{ok, PaloadTopic, Action, Data} ->
					{M, S, MS} = Message#message.timestamp,
					KafkaPayload = [
							{clientId , Message#message.from},
							{appId , get_app_id(Username)},
							{recvedAt , timestamp()},
							{from , <<"mqtt">>},
							{type , <<"string">>},
							{msgId , gen_msg_id(Event, Clientid)},
							{mqttTopic , Topic},
							{topic , PaloadTopic},
							{action , Action},
							{timestamp , (M * 1000000 + S) * 1000 + MS div 1000},
							{data , Data}
						],
					case get_kafka_config(Event, Message#message.from) of
						{ok, KafkaTopic, Partition, Client} ->
							KafkaMessage = jsx:encode(KafkaPayload),
%% 							ok = brod:produce_sync(Client, KafkaTopic, Partition, <<>>, KafkaMessage),
							safe_send(Client, KafkaTopic, Partition, KafkaMessage),
							log_kafka(info, LoggerHeader, "[Kafka] msg payload: ~s topic:~s", [KafkaMessage, KafkaTopic]);
						{error, Msg} -> 
							log_kafka(error, LoggerHeader, "[Kafka] get_kafka_config error: ~s", [Msg])
					end;
				{error, Msg} ->
					log_kafka(info, LoggerHeader, "[Kafka] msg kafka body error: ~s payload:~s", [Msg, Message#message.payload])
			end;
		{error, Msg} ->
			log_kafka(debug, LoggerHeader, "[Kafka] process topic error: ~s", [Msg])
	end,
    ok;

produce_message_kafka_payload(_Message = #message{headers = #{username:=_Username}}) ->
	ok.

log_kafka(Level, Header, Msg, Args) ->
	?LOG(Level, "~s " ++ Msg , [Header] ++ Args).

timestamp() ->
    {M, S, MS} = os:timestamp(),
    (M * 1000000 + S) * 1000 + MS div 1000.

gen_msg_id(connected, ClientId)->
	list_to_binary("rbc" ++ string:substr(md5:md5(binary_to_list(ClientId) ++ integer_to_list(timestamp() + rand:uniform(1000000))), 8, 20));

gen_msg_id(disconnected, ClientId)->
	list_to_binary("rbd" ++ string:substr(md5:md5(binary_to_list(ClientId) ++ integer_to_list(timestamp() + rand:uniform(1000000))), 8, 20));

gen_msg_id(custom, ClientId)->
	list_to_binary("rbt" ++ string:substr(md5:md5(binary_to_list(ClientId) ++ integer_to_list(timestamp() + rand:uniform(1000000))), 8, 20));

gen_msg_id(event, ClientId)->
	list_to_binary("rbe" ++ string:substr(md5:md5(binary_to_list(ClientId) ++ integer_to_list(timestamp() + rand:uniform(1000000))), 8, 20)).

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
			<<"">>;
		_->
			list_to_binary(lists:nth(2,string:tokens(UsernameStr,"@")))
	end.

get_mqtt_topic(Clientid, connected)->
	NodeStr = string:concat("$SYS/brokers/", atom_to_list(node())),
	Result =  string:concat(string:concat(string:concat(NodeStr, "/clients/"), binary_to_list(Clientid)), "/connected"),
	list_to_binary(Result);

get_mqtt_topic(Clientid, disconnected)->
	NodeStr = string:concat("$SYS/brokers/", atom_to_list(node())),
	Result =  string:concat(string:concat(string:concat(NodeStr, "/clients/"), binary_to_list(Clientid)), "/disconnected"),
	list_to_binary(Result).

get_ip_str({{I1, I2, I3, I4}, Port})->
	IpStr = integer_to_list(I1) ++ "." ++ integer_to_list(I2) ++ "." ++ integer_to_list(I3) ++ "." ++ integer_to_list(I4),
	IP = list_to_binary(IpStr),
	{IP, list_to_binary(IpStr ++ ":" ++ integer_to_list(Port))}.

is_online(connected)->
	true;

is_online(disconnected)->
	false.
	
safe_send(Client, Topic, Partition, KafkaMessage) ->
	case brod:produce_sync(Client, Topic, Partition, <<>>, KafkaMessage) of
		ok -> ok;
		{error, Result} ->
			?LOG(error, "[kafka] produce error: ~p", [Result])
	end.

produce_online_kafka_log(Clientid, Username, Peername, Connection) ->
	Now = timestamp(),
	MqttTopic = get_mqtt_topic(Clientid, Connection),
	{Ip, IpPort} = get_ip_str(Peername),
	KafkaPayload = [
					{clientId , Clientid},
					{appId , get_app_id(Username)},
					{recvedAt , Now},
					{msgId , gen_msg_id(Connection, Clientid)},
					{mqttTopic , MqttTopic},
					{deviceSource, <<"roobo">>},
					{from, <<"mqtt">>},
					{action , <<"device.status.online">>},
					{ipaddress , Ip},
					{timestamp , Now},
					{isOnline , is_online(Connection)},
					{username , Username}
				],
	[{_,Topic}] = ets:lookup(kafka_config, online_topic),
	[{_, PartitionTotal}] = ets:lookup(kafka_config, online_partition_total),
	Partition = erlang:phash2(Clientid) rem PartitionTotal,
	KafkaMessage = jsx:encode(KafkaPayload),
	safe_send(online_client, list_to_binary(Topic), Partition, KafkaMessage),
%% 	ok = brod:produce_sync(online_client, list_to_binary(Topic), Partition, <<>>, KafkaMessage),
	?LOG(info, "~s@~s [Kafka] pid:~s ~p payload: ~s topic:~s", [Clientid, IpPort, pid_to_list(self()), Connection, KafkaMessage, Topic]),
    ok.

%% Called when the plugin application stop
unload() ->
	brod:stop_client(event_client),
	brod:stop_client(online_client),
	brod:stop_client(custom_client),
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

