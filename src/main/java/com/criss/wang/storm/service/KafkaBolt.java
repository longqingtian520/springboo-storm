package com.criss.wang.storm.service;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author wangqiubao
 * @Date 2019/9/30 15:04
 * @Description
 **/
public class KafkaBolt implements IRichBolt {

    private Logger logger = LoggerFactory.getLogger(KafkaBolt.class);


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        logger.info("topic: {}, \t分区：{}，\t 偏移量：{}，\t key值：{}，\t 消息：{} \t",
                tuple.getValues().get(0).toString(), tuple.getValues().get(1).toString(), tuple.getValues().get(2).toString(),
                tuple.getValues().get(3) == null ? "": tuple.getValues().get(3).toString(), tuple.getValues().get(4).toString());

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
