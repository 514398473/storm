package com.xz.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt{

	private static final long serialVersionUID = -612944903812182105L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ReportBolt.class);
	
	private Map<String, Long> counts;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		counts = new HashMap<String,Long>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		counts.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public void cleanup() {
		LOGGER.info("--------------------------------------------------------------------------------------");
		List<String> keys = new ArrayList<String>();
		keys.addAll(counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			LOGGER.info(key + ":" + counts.get(key));
		}
		LOGGER.info("计算完成！");
	}

}
