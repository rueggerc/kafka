package com.rueggerllc.kafka.simple;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;


public class SimpleConsumerApp {
	
	private static final Logger logger = Logger.getLogger(SimpleConsumerApp.class);
	
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092";

    public static void main(String[] args) {
    	
    	logger.info("==========RUGE! SIMPLE CONSUMER STARTUP ==========");
    	 
        // Consumer properties
        Properties props = new Properties();
        // props.put("bootstrap.servers", "localhost:9092");
        // props.put("bootstrap.servers", "192.168.1.253:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put("zookeeper.connect", "captain:2181");
        
        props.put("group.id", "test-group");
        
        // Using auto commit
        props.put("enable.auto.commit", "false");
 
        //string inputs and outputs
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 
        //kafka consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        logger.info("Consumer Built");
 
        //subscribe to topic
        consumer.subscribe(Arrays.asList("orders"));
        // consumer.seekToBeginning(partitions);
 
        // Infinite poll loop
        while (true) {
        	logger.info("IN MAIN LOOP");
            ConsumerRecords<String, String> records = consumer.poll(100);
            logger.info("GOT DATA");
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
 
    }

}

