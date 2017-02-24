package com.sinoiov.yyzc.commons.kafka;

import java.io.File;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinoiov.yyzc.commons.kafka.util.EnvironmentUtil;
import com.sinoiov.yyzc.commons.kafka.util.JSONUtil;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class YyzcKafkaBasicProducer {
	
	private final static Logger logger = LoggerFactory.getLogger(YyzcKafkaBasicProducer.class);
	
	private static String default_path = "/opt/web_app/config/";
	
	private static String location = "kafka-producer.properties";
	
	private static Properties props;
	
	private Producer<Integer, String> producer;
	
	public YyzcKafkaBasicProducer() {
		File file = new File(default_path + location);
		location = file.isFile()?default_path + location : location;
		props = EnvironmentUtil.getInstance(location).getProperties();
		if(producer == null)
			producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	public void send(String topic, Object message) throws Exception {
		try{
			String json = JSONUtil.object2JSON(message);
			logger.info("Sending message to topic [{}], message is [{}].", topic, json);
			producer.send(new KeyedMessage<Integer, String>(topic, json));
		}catch(Exception e){
			logger.warn("--->>>send message exception:", e);
			throw e;
		}finally{
			//建议关闭，否则会造成系统连接数不断增加
			if(producer!=null)
				producer.close();
		}
		
	}
}
