/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.storm.test.wordcount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年7月3日
 */

public class WordCountTopology {

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("mySpout", new WordCountSpout(), 3);
		topologyBuilder.setBolt("myBolt1", new SplitBolt(), 4).shuffleGrouping("mySpout");
		topologyBuilder.setBolt("myBolt2", new CountBolt(), 5).fieldsGrouping("myBolt1", new Fields("word"));

		Config config = new Config();
		config.setNumWorkers(2);

		//本地模式
//		LocalCluster localCluster = new LocalCluster();
//		localCluster.submitTopology("wordcount", config, topologyBuilder.createTopology());

		//集群模式
		StormSubmitter.submitTopology("wordcount", config, topologyBuilder.createTopology());
		
	}

}
