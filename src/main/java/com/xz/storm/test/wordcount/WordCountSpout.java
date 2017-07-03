/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.storm.test.wordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
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

public class WordCountSpout extends BaseRichSpout {

	/**  */
	private static final long serialVersionUID = 2337818850484444629L;
	private SpoutOutputCollector collector;

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		collector.emit(new Values("test storm word count"));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}

}
