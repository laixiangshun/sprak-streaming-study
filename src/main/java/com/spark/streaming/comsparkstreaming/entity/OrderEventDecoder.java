package com.spark.streaming.comsparkstreaming.entity;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class OrderEventDecoder implements Deserializer<OrderEvent> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	

	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public OrderEvent deserialize(String topic, byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, OrderEvent.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	
	}

}

