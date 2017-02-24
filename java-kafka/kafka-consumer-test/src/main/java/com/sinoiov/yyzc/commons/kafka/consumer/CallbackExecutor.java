package com.sinoiov.yyzc.commons.kafka.consumer;

import org.codehaus.jackson.type.TypeReference;

import com.sinoiov.yyzc.commons.kafka.YyzcKafkaMessageExecutor;
import com.sinoiov.yyzc.commons.kafka.consumer.message.MessageCallback;
import com.sinoiov.yyzc.commons.kafka.exceptions.YyzcKafkaException;
import com.sinoiov.yyzc.commons.kafka.util.JSONUtil;

public class CallbackExecutor implements YyzcKafkaMessageExecutor {

	@Override
	public void execute(Object message) throws YyzcKafkaException {
		// TODO Auto-generated method stub
		try {
			String json = JSONUtil.object2JSON(message);
			System.out.println("【executor】" + json);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public TypeReference transferToObjectClass() throws YyzcKafkaException {
		// TODO Auto-generated method stub
		return new TypeReference<MessageCallback>() {};
	}
}
