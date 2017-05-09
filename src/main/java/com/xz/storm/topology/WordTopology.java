package com.xz.storm.topology;

import javax.swing.plaf.basic.BasicBorders.SplitPaneBorder;

import com.xz.storm.bolt.CountBolt;
import com.xz.storm.bolt.ReportBolt;
import com.xz.storm.bolt.SpiltBolt;
import com.xz.storm.spout.WordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordTopology {

	public static void main(String[] args) {
		WordSpout wordSpout = new WordSpout();
		SpiltBolt spiltBolt = new SpiltBolt();
		CountBolt countBolt = new CountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("wordSpout", wordSpout);
		topologyBuilder.setBolt("spiltBolt", spiltBolt).shuffleGrouping("wordSpout");
		topologyBuilder.setBolt("countBolt", countBolt).fieldsGrouping("spiltBolt", new Fields("word"));
		topologyBuilder.setBolt("reportBolt", reportBolt).globalGrouping("countBolt");
		
		Config config = new Config();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("wordCount",config,topologyBuilder.createTopology());
		Utils.sleep(10000);
		localCluster.killTopology("wordCount");
		localCluster.shutdown();
	}
}
