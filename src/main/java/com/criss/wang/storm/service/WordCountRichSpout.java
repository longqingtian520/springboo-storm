package com.criss.wang.storm.service;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountRichSpout extends BaseRichSpout{

	/**
	 *
	 */
	private static final long serialVersionUID = -4016616772077996605L;

	private SpoutOutputCollector controller;

	@Override
	public void nextTuple() {
		controller.emit(new Values("We can do better , yes"));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector controller) {
		this.controller = controller;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("better"));
	}

}
