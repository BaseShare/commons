package com.sinoiov.yyzc.commons.kafka;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sinoiov.yyzc.commons.kafka.util.EnvironmentUtil;
import com.sinoiov.yyzc.commons.kafka.util.JSONUtil;

import kafka.javaapi.consumer.ConsumerConnector;

public class YyzcKafkaBasicConsumer {
	
	private final static Logger logger = LoggerFactory.getLogger(YyzcKafkaBasicConsumer.class);
	
	protected ConsumerConnector consumer;
	
	private static String default_path = "/opt/web_app/config/";
	
	private String location = "kafka-consumer.properties";// 配置文件位置
	
	private ExecutorService threadPool;
	
	public void init() throws Exception {
		if(StringUtils.isNotBlank(authConfPath)) System.setProperty("java.security.auth.login.config", authConfPath);
		File file = new File(default_path + location);
		location = file.isFile()?default_path + location : location;
		Properties properties = new EnvironmentUtil(location).getProperties();
		if (executor == null) {
			throw new RuntimeException("KafkaConsumer, exectuor cant be null!");
		}
		if (topic == null) {
			throw new RuntimeException("KafkaConsumer, topic cant be null!");
		}
		if(!StringUtils.isBlank(type) && type.equals("SUB")){//订阅模式，groupid为空，默认使用进程号+机器名，不为空，则使用之。
			String localGroupId = ManagementFactory.getRuntimeMXBean().getName().split("@")[1] + "_"
						+ YyzcKafkaBasicConsumer.class.getClassLoader().getResource("").getPath().replaceAll("/", "_");
			properties.put("group.id", StringUtils.isBlank(groupid) ? localGroupId : groupid);
		}
		
		logger.info("Starting initializate Kafka Consumer with topic [{}] and groupId [{}]......", topic, properties.getProperty("group.id"));
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		threadPool = Executors.newFixedThreadPool(partitionsNum);
		threadPool.execute(new MessageRunner(consumer, toJson));
	}

	public void close() {
		try {
			threadPool.shutdownNow();
		} catch (Exception e) {
			//
		} finally {
			consumer.shutdown();
		}

	}
	
	//inner class or interface
	class MessageRunner implements Runnable {
		private String toJson;
		private KafkaConsumer<String, String> consumer; 

		MessageRunner(KafkaConsumer<String, String> consumer, String toJson) {
			this.consumer = consumer;
			this.toJson = toJson;
		}
		
		public void run() {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records){
					logger.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
					try{
						if(StringUtils.isNotBlank(toJson) && toJson.equalsIgnoreCase("no")) executor.execute(record.value());
						else executor.execute(JSONUtil.json2Object(record.value(), executor.transferToObjectClass()));
					}catch(Exception e){
						logger.error("Error in deal with message: ", e);
					}
				}
			}
		}
	}
	
	//spring 注入
	protected String topic;
	
	private String type;

	protected String groupid;

	private int partitionsNum = 1;
	
	private String toJson;
	
	private String authConfPath;

	private YyzcKafkaMessageExecutor<?> executor; // message listener
	
	public void setExecutor(YyzcKafkaMessageExecutor<?> executor) {
		this.executor = executor;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setGroupid(String groupid) {
		this.groupid = groupid;
	}

	public void setPartitionsNum(int partitionsNum) {
		this.partitionsNum = partitionsNum;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setToJson(String toJson) {
		this.toJson = toJson;
	}

	public String getAuthConfPath() {
		return authConfPath;
	}

	public void setAuthConfPath(String authConfPath) {
		this.authConfPath = authConfPath;
	}
}
