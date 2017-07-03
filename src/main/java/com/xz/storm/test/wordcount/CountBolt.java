/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.storm.test.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年7月3日
 */

public class CountBolt extends BaseRichBolt {

	/**  */
	private static final long serialVersionUID = 6233497930654962684L;
	private Map<String, Integer> map = new HashMap<String, Integer>();

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		Integer num = input.getInteger(1);
		if (map.containsKey(word)) {
			Integer count = map.get(word);
			map.put(word, count + num);
		} else {
			map.put(word, num);
		}
		System.out.println("count:" + map);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
