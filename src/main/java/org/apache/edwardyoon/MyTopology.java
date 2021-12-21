package org.apache.edwardyoon;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MyTopology {

    public static final void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MQTTSpout(
                "tcp://mqtt_address:1883", "test"), 1);

        StormSubmitter.submitTopology("mytest", new Config(), builder.createTopology());
    }
}
