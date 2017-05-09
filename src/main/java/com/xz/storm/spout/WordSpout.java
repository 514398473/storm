package com.xz.storm.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordSpout extends BaseRichSpout {

	private static final long serialVersionUID = -8334902390873558122L;

	private SpoutOutputCollector spoutOutputCollector;

	private String[] sentences = { "A Storm cluster is superficially similar to a Hadoop cluster",
			"Whereas on Hadoop you run", "one key difference is that a MapReduce job eventually finishes",
			"whereas a topology processes messages forever", "There are two kinds of nodes on a Storm cluster",
			"the master node and the worker nodes", "The master node runs a daemon called Nimbus",
			"Nimbus is responsible for distributing code around the cluster",
			"The supervisor listens for work assigned to its machine and starts and stops worker processes as necessary based on what Nimbus has assigned to it",
			"Each worker process executes a subset of a topology", "my dog has fleas", "i like cold beverages",
			"the dog ate my homework", "don't have a cow man", "i don't think i like fleas" };

	private int index = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.spoutOutputCollector = collector;
	}

	@Override
	public void nextTuple() {
		spoutOutputCollector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentences"));
	}

}
