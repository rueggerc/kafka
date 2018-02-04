package com.rueggerllc.kafka.stream;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


public class StreamProducerApp {
	
	private static final Logger logger = Logger.getLogger(StreamProducerApp.class);
	private static String tickers[] = {"NVDA", "FB", "AMZN", "BABA"};
    
    public static void main(String[] argv)throws Exception {
    	
    	try {
	        if (argv.length != 1) {
	            logger.error("Please specify Topic Name");
	            System.exit(-1);
	        }
	        String topicName = argv[0];
	  
	
	        // Configure the Producer
	        Properties configProperties = new Properties();
	        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
	        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
	        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String,String> producer = new KafkaProducer<>(configProperties);
	        
	        for (int i = 0; i < 300; i++) {
	        	
	        	String ticker = tickers[i%tickers.length];
	        	
	        	Thread.currentThread().sleep(1000);
	        	double value = Math.random() * 100;
	        	String line = String.format("%d %s %f", i, ticker, value);
	        	System.out.println("Sending: " + line);
	            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
	            producer.send(rec);
	        }
	        producer.close();
    	} catch (Exception e) {
        	System.out.println("ERROR:\n" + e);
        }
    }
}
