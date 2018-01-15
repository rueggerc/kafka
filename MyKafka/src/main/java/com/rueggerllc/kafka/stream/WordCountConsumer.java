package com.rueggerllc.kafka.stream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;


public class WordCountConsumer {
	
	private static final Logger logger = Logger.getLogger(WordCountConsumer.class);
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092";
	private KafkaConsumer<String,String> kafkaConsumer;
	
	public void execute(String topicName, String groupName) throws Exception {
		try {
			
	        logger.info("============== WordCountConsumer BEGIN ==============");
	        Properties configProperties = new Properties();
	        // configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
	        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "wordCount");
	        
	        // Create Consumer and subscribe to Topic
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            	        
	        // Get Messages
	        logger.info("Polling For Messages...");
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                	logger.info(record.value());
            }			
			
		} catch (Exception e) {
			logger.error("EROR", e);
		}  finally {
			if (kafkaConsumer != null) {
				kafkaConsumer.close();
			}
		}
	}
	
	
    public static void main(String[] argv) {
    	try {
    		
	        if (argv.length != 2) {
	            logger.error(String.format("Usage: %s <topicName> <groupId>\n", WordCountConsumer.class.getSimpleName()));
	            System.exit(-1);
	        }
	        String topicName = argv[0];
	        String groupName = argv[1]; 
	        
	        WordCountConsumer consumer = new WordCountConsumer();
	        consumer.execute(topicName, groupName);
	       
    	} catch (Exception e) {
    		logger.error("Error", e);
    	}

    }

}

