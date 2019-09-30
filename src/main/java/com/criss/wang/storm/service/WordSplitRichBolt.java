package com.criss.wang.storm.service;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordSplitRichBolt extends BaseRichBolt{

	/**
	 *
	 */
	private static final long serialVersionUID = 1389852554806073410L;

	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		String wordStr = input.getString(0);
		String[] splits = wordStr.split(" ");
		for(String word : splits) {
			collector.emit(new Values(word, 1));
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector controller) {
		this.collector = controller;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "num"));
	}

}
