package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.kafka.consumer.Consumer;
import com.example.kafka.stream.consumer.StreamConsumer;

@SpringBootApplication
public class KafkaConsumerApplication implements CommandLineRunner{

	
	@Autowired
	private StreamConsumer streamConsumer;
	
	@Autowired
	private Consumer consumer;
	
	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
	
		// consumer.consumeMessages("hello");
		streamConsumer.consumeMessages();
	}

}
