package com.criss.wang.storm;

import com.criss.wang.storm.service.KafkaBolt;
import com.criss.wang.storm.service.WordCountRichSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
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
import com.criss.wang.storm.service.WordSplitRichBolt;

import java.util.HashMap;
import java.util.Map;

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
//		stormDemo(args);
		kafkaStormDemo(args);
	}

	public static void stormDemo(String[] args) throws Exception{
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

	public static void kafkaStormDemo(String[] args) throws Exception {
		KafkaSpoutConfig.Builder<String,String> builder = KafkaSpoutConfig.builder("192.168.3.37:9092","criss-test");

		Map<String, Object> map = new HashMap<>();
		map.put("group.id", "test_storm"); // 更换groupId可以产生从头消费的效果
		map.put("auto.offset.reset", "latest");
		map.put("enable.auto.commit", true);

		builder.setProp(map);
//		kafka.api.OffsetRequest.LatestTime();
//		kafka.api.OffsetRequest.EarliestTime();

		KafkaSpoutConfig<String, String> kafkaSpoutConfig= builder.build();
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		KafkaSpout spout = new KafkaSpout<>(kafkaSpoutConfig);
		topologyBuilder.setSpout("WordCountFileSpout",spout, 1);
		topologyBuilder.setBolt("readKafkaBolt",new KafkaBolt()).shuffleGrouping("WordCountFileSpout");
		Config config = new Config();
		if(args !=null && args.length > 0){
			config.setDebug(false);
			StormSubmitter submitter= new StormSubmitter();
			submitter.submitTopology("kafkaStromTopo", config, topologyBuilder.createTopology());
		}else{
			config.setDebug(false);
			LocalCluster cluster= new LocalCluster();
			cluster.submitTopology("kafkaStromTopo", config, topologyBuilder.createTopology());
		}
	}

}
