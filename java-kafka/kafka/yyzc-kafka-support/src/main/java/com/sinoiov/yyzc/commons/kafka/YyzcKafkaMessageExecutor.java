package com.sinoiov.yyzc.commons.kafka;

import org.codehaus.jackson.type.TypeReference;

public interface YyzcKafkaMessageExecutor<T extends Object> {

	public void execute(Object message) throws Exception;
	
	public TypeReference<T> transferToObjectClass() throws Exception;
	
}
