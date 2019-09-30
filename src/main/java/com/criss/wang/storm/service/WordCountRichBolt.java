package com.criss.wang.storm.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class WordCountRichBolt extends BaseRichBolt{

	/**
	 *
	 */
	private static final long serialVersionUID = -3368646551400064944L;

	private OutputCollector collector;

	private Map<String, Integer> map = new HashMap<>();

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		int num = input.getInteger(1);
		if(map.containsKey(word)) {
			num = map.get(word).intValue() + num;
			map.put(word, num);
		}else {
			map.put(word, num);
		}

		System.out.println("线程Id：" + Thread.currentThread().getId() + ", word:" + word + ", num:" + map.get(word));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
