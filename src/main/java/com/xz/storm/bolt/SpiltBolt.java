package com.xz.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SpiltBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3570877610070360232L;

	private OutputCollector outputCollector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;

	}

	@Override
	public void execute(Tuple input) {
		String sentences = input.getStringByField("sentences");
		String[] words = sentences.split(" ");
		for (String word : words) {
			outputCollector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
