/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.storm.test.wordcount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年7月3日
 */

public class SplitBolt extends BaseRichBolt {

	/**  */
	private static final long serialVersionUID = 9013777038725793210L;
	private OutputCollector collector;

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		String[] words = line.split(" ");
		for (String word : words) {
			collector.emit(new Values(word, 1));
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "num"));
	}

}
