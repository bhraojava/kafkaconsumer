package com.example.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;



@Service
public class Consumer {
	
private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
	
	private static final String TOPIC= "TestTopic";
	
	//@KafkaListener(topics = TOPIC)
	public void consumeMessages(String message) {
		logger.info("consume messages from kafka topic "+ TOPIC + "is  " + message);
	}
}
