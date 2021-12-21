/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.edwardyoon;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.*;

import org.apache.storm.topology.IRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;

public class MQTTSpout implements MqttCallback, IRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(MQTTSpout.class);
    MqttClient client;
    SpoutOutputCollector collector;
    LinkedList<String> messages;

    String broker_url;
    String topic;
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public MQTTSpout(String broker_url, String topic) {
        this.broker_url = broker_url;
        this.topic = topic;
        messages = new LinkedList<String>();
    }

    public void messageArrived(String topic, MqttMessage message)
            throws Exception {
        LOG.info("Logging tuple with logger: " + topic + ", " + message);

        messages.add(message.toString());
    }

    public void connectionLost(Throwable cause) {
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            client = new MqttClient(broker_url, MqttClient.generateClientId());
            MqttConnectOptions connOpts = setUpConnectionOptions(USERNAME, PASSWORD);
            client.connect(connOpts);

            client.setCallback(this);
            client.subscribe(topic);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static MqttConnectOptions setUpConnectionOptions(String username, String password) {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setUserName(username);
        connOpts.setPassword(password.toCharArray());
        return connOpts;
    }

    public void close() {
    }

    public void activate() {
    }

    public void deactivate() {
    }

    public void nextTuple() {
        while (!messages.isEmpty()) {
            collector.emit(new Values(messages.poll()));
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO https://issues.apache.org/jira/browse/STORM-3611
        // declarer.declare(new Fields("start", "end", "result"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
