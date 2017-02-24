package com.sinoiov.yyzc.commons.kafka.consumer;

import org.codehaus.jackson.type.TypeReference;

import com.sinoiov.yyzc.commons.kafka.YyzcKafkaMessageExecutor;

public class TestExecutor implements YyzcKafkaMessageExecutor {

	@Override
	public void execute(Object message) {
		// TODO Auto-generated method stub
		System.out.println(message);
	}

	@Override
	public TypeReference transferToObjectClass() {
		// TODO Auto-generated method stub
		return new TypeReference<String>() {};
	}

}
