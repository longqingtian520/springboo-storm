package com.criss.wang.storm.service;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("WordCountRichSpout", new WordCountRichSpout(), 1);
		topologyBuilder.setBolt("WordSplitRichBolt", new WordSplitRichBolt(), 4).fieldsGrouping("WordCountRichSpout", new Fields("better"));
		topologyBuilder.setBolt("WordCountRichBolt", new WordCountRichBolt(), 2).fieldsGrouping("WordSplitRichBolt", new Fields("word"));

		Config config = new Config();

		if(args.length > 0) {
			// 集群启动
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
		}else {
			// 本地启动
			LocalCluster local = new LocalCluster();
			local.submitTopology("wordcounttopology", config, topologyBuilder.createTopology());
		}
	}
}
