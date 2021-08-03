package com.example.kafka.stream.consumer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class StreamConsumer {

	private static final Logger logger = LoggerFactory.getLogger(StreamConsumer.class);

	private static final String TOPIC = "TestTopic";

	private String latestTime;
	private long totalTime;
	private double distance;
	private double totalDistance;
	private String preLat = null;
	private String preLng = null;

	public void consumeMessages() {
		logger.info("consume messages from kafka topic " + TOPIC + "streaming");

		Properties properties = new Properties();

		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consume");
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> kStream = builder.stream(TOPIC);

		kStream.foreach((key, value) -> {
			logger.info("key ============   " + key);
			logger.info("value ============   " + value);
			readValues(value);
		});

//		kStream
//	        .map((k, v) -> new KeyValue<>(readKeys(v), readValues(v)))
//	        // Group by title
//	        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
//	        // Apply SUM aggregation
//	        .reduce(Double::sum)
//	        .toStream()
//	        .foreach((key, value) -> {
//	        	logger.info("key ============   " + key);
//				logger.info("value ============   " + value);
//	        });

		final Topology topology = builder.build();

		final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("stream-shutdown-hook") {
			@Override
			public void run() {
				kafkaStreams.close();
				latch.countDown();
			}
		});

		try {
			kafkaStreams.start();
			latch.await();
		} catch (Throwable th) {
			System.exit(1);
		}
		System.exit(0);
	}

	private String readKeys(String value) {
		String[] messages = value.split(",");
		return messages[2].trim();
	}

	private void readValues(String value) {
		String[] messages = value.split(",");
		String lat = messages[0].trim();
		String lng = messages[1].trim();
		String time = messages[2].trim();

		if (preLat == null && preLng == null) {
			latestTime = time;
			distance = 0;
			preLat = lat;
			preLng = lng;
			totalDistance = totalDistance + distance;
		} else {
			getTotalTime(time,  latestTime);  
			latestTime = time;
			distance = distance(Double.valueOf(lat), Double.valueOf(lng), Double.valueOf(preLat),
					Double.valueOf(preLng), 'K');
			preLat = lat;
			preLng = lng;
			totalDistance = totalDistance + distance;
		}
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
		LocalTime localtime = LocalTime.MIN.plusSeconds(totalTime);
   	 	String  totalTimeCovered = formatter.format(localtime);
		logger.info("current Time:   " + latestTime + " totalTime:  "+ totalTimeCovered
				+" current distance in KM:  " + distance
				+ " totalDistanceCovered in KM == " + totalDistance);

	}

	private void getTotalTime(String time, String latestTime2) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");  
		Date d1 = null;
		Date d2 = null;
		try {
		    d1 = format.parse(time);
		    d2 = format.parse(latestTime2);
		} catch (ParseException e) {
		    e.printStackTrace();
		}
		 long diff = d1.getTime() - d2.getTime();
    	 long seconds = TimeUnit.MILLISECONDS.toSeconds(diff);
    	 totalTime = totalTime + seconds;
    	 // logger.info("calculated time is : "+ time + " - " + " "+ latestTime2 + " : "+ seconds +  "totalTime : "+ totalTime);
	}

	private double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == 'K') {
			dist = dist * 1.609344;
		} else if (unit == 'M') {
			dist = dist * 1.609344;
			dist = dist * 1000;
		}
//		logger.info("calculated Distance : " + dist);
		return (dist);
	}

	private double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	private double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}
}
