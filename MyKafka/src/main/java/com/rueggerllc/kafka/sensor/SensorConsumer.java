package com.rueggerllc.kafka.sensor;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;


public class SensorConsumer {
	
	private static final Logger logger = Logger.getLogger(SensorConsumer.class);
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092";
    private static Scanner in;

    public static void main(String[] argv)throws Exception{
        if (argv.length != 2) {
            logger.error(String.format("Usage: %s <topicName> <groupId>\n", SensorConsumer.class.getSimpleName()));
            System.exit(-1);
        }
        
        logger.info("============== SIMPLE CONSUMER BEGIN ==============");
        
        
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        // Start Thread
        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        
        // Wait or user to type "exit" to shutdown
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        logger.info("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            // configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            // Create Consumer and subscribe to Topic
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            
            
            // Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                    	logger.info(record.value());
                    }
                }
            } catch(WakeupException e){
                logger.error("Exception caught " + e);
            } finally{
                kafkaConsumer.close();
                logger.error("After closing KafkaConsumer");
            }
        }
        
        public KafkaConsumer<String,String> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
    }
}

