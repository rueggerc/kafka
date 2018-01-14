package com.rueggerllc.kafka.partition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;


public class ConsumerApp {
	
	private static final Logger logger = Logger.getLogger(ConsumerApp.class);
    private static Scanner in;
    private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092";
 
    public static void main(String[] argv) throws Exception {
        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n", ConsumerApp.class.getSimpleName());
            System.exit(-1);
        }
        
        logger.info("Partition ConsumerApp Startup");
        
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        // Start Main Thread
        logger.info("Consumer Started topic=" + topicName + " Group=" + groupId);
        ConsumerThread consumerThread = new ConsumerThread(topicName, groupId);
        consumerThread.start();
        
        logger.info("Main Thread Started");
        
        // Wait for Exit command
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerThread.getKafkaConsumer().wakeup();
        logger.info("Stopping consumer .....");
        consumerThread.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() {
        	try {
	        	System.out.println("=== Consumer Thread Startup topic=" + topicName + " Group=" + groupId);
	        	logger.info("BROKERS=" + BROKERS);
	            Properties configProperties = new Properties();
	            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "JavaClient");
	
	            
	            // Figure out where to start processing messages from
	            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
//	            System.out.println("CONSUMER BUILT");
//	            System.out.println("TOPIC NAME=" + topicName);
	            
//	            List<String> topics = new ArrayList<String>();
//	            topics.add(topicName);
//	            kafkaConsumer.subscribe(topics);
	            
	       
	            
//	            Map<String, List<PartitionInfo>> myTopics = kafkaConsumer.listTopics();
//	            for (Map.Entry<String, List<PartitionInfo>> entry : myTopics.entrySet()) {
//	            	logger.info("NEXT KEY= " + entry.getKey());
//	            }
//	            
//	            Set<String> mysubscriptions = kafkaConsumer.subscription();
//	            for (String next : mysubscriptions) {
//	            	logger.info("Next subscription=" + mysubscriptions);
//	            }
	            
	            
	            kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
	                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
	                	logger.info(String.format("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray())));
	                }
	                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
	                    logger.info(String.format("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray())));
	                }
	            });
	            
	            logger.info("===== PARTITION CONSUMER WAITING FOR MESSAGES =====");
	            // Start processing messages
	            try {
	                while (true) {
	                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
	                    for (ConsumerRecord<String, String> record : records) {
	                        logger.info(record.value());
	                    }
	                }
	            } catch (WakeupException ex) {
	                logger.error("Exception caught " + ex.getMessage());
	            } finally {
	                kafkaConsumer.close();
	                logger.info("After closing KafkaConsumer");
	            }
        	} catch (Exception e) {
        		System.out.println("ERROR:\n" + e);
        	}
        	System.out.println("WE ARE OUTTA HERE");
        	
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }
}


