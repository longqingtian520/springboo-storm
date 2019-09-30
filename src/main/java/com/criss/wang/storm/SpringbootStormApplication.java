package com.criss.wang.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.springframework.beans.BeansException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.criss.wang.storm.service.WordCountRichBolt;
import com.criss.wang.storm.service.WordCountRichSpout;
import com.criss.wang.storm.service.WordSplitRichBolt;

@SpringBootApplication
public class SpringbootStormApplication implements ApplicationContextAware, CommandLineRunner{

	private ApplicationContext applicationContext;

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(SpringbootStormApplication.class);
		app.addListeners(new ApplicationPidFileWriter());
        app.run(args);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void run(String... args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("WordCountRichSpout", new WordCountRichSpout(), 1);
		topologyBuilder.setBolt("WordSplitRichBolt", new WordSplitRichBolt(), 4).fieldsGrouping("WordCountRichSpout", new Fields("better"));
		topologyBuilder.setBolt("WordCountRichBolt", new WordCountRichBolt(), 1).fieldsGrouping("WordSplitRichBolt", new Fields("word"));

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
