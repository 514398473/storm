package com.xz.kafka.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaAndStormTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("study1:2181,study2:2181,study3:2181"), "test",
				"/myKafka", "kafkaSpout");
		topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), 1);
		topologyBuilder.setBolt("kafkaBolt", new MyBolt(), 1).shuffleGrouping("kafkaSpout");

		Config config = new Config();
		config.setNumWorkers(1);

		if (args.length > 0) {
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
		} else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("storm2kafka", config, topologyBuilder.createTopology());
		}
	}
	
}
