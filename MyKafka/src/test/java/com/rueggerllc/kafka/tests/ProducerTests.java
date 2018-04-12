package com.rueggerllc.kafka.tests;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ProducerTests {

	private static Logger logger = Logger.getLogger(ProducerTests.class);
	private static String tickers[] = {"NVDA", "FB", "AMZN", "BABA"};
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";

	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	// @Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	public void testWriteTickers() {
		try {
			
	        String topicName = "ticker-topic";
	    	
	        // Configure the Producer
	        Properties configProperties = new Properties();
	        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKERS);
	        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
	        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String,String> producer = new KafkaProducer<>(configProperties);
	        
	        for (int i = 0; i < 10; i++) {
	        	
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
			
		}
	}
	
	
}
