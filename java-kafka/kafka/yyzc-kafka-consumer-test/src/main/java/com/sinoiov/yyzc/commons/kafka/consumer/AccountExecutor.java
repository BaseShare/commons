package com.sinoiov.yyzc.commons.kafka.consumer;

import org.codehaus.jackson.type.TypeReference;

import com.sinoiov.yyzc.commons.kafka.YyzcKafkaMessageExecutor;
import com.sinoiov.yyzc.commons.kafka.consumer.message.MessageAccount;
import com.sinoiov.yyzc.commons.kafka.exceptions.YyzcKafkaException;

public class AccountExecutor implements YyzcKafkaMessageExecutor {
	
	@Override
	public void execute(Object message) throws YyzcKafkaException {
		// TODO Auto-generated method stub
		System.out.println("已收到【ACCOUNT】消息：" + ((MessageAccount)message).getAccountCode());
	}

	@Override
	public TypeReference transferToObjectClass() throws YyzcKafkaException {
		// TODO Auto-generated method stub
		return new TypeReference<MessageAccount>() {};
	}

}
