package com.sinoiov.yyzc.commons.kafka.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.type.TypeReference;

public final class JSONUtil {
	
	static public String object2JSON(Object object) throws Exception {
		return new ObjectMapper().setSerializationInclusion(Inclusion.NON_EMPTY).writeValueAsString(object);
	}

	static public Object json2Object(String json, TypeReference<?> typeReference) throws Exception {
		return new ObjectMapper().readValue(json, typeReference);
	}
}