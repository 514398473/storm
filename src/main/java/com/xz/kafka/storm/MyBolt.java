package com.xz.kafka.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4688747082907341574L;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		byte[] bytes = (byte[]) input.getValue(0);
		String value = new String(bytes);
		String[] words = value.split(" ");
		for (String word : words) {
			System.out.println(word);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
