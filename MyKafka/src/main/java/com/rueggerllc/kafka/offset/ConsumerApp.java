package com.rueggerllc.kafka.offset;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
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

    public static void main(String[] argv)throws Exception{
        if (argv.length != 3) {
            logger.error(String.format("Usage: %s <topicName> <groupId> <startingOffset>\n", ConsumerApp.class.getSimpleName()));
            System.exit(-1);
        }
        in = new Scanner(System.in);

        String topicName = argv[0];
        String groupId = argv[1];
        final int startingOffset = Integer.parseInt(argv[2]);

        ConsumerThread consumerThread = new ConsumerThread(topicName,groupId,startingOffset);
        consumerThread.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerThread.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerThread.join();

    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private int startingOffset;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId, int startingOffset){
            this.topicName = topicName;
            this.groupId = groupId;
            this.startingOffset=startingOffset;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
            configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            // configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            // Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                	logger.info(String.format("%s topic-partitions REVOKED from this consumer\n", Arrays.toString(partitions.toArray())));
                }
                
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.info(String.format("%s topic-partitions ASSIGNED to this consumer\n", Arrays.toString(partitions.toArray())));
                    
                    Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
                    while (topicPartitionIterator.hasNext()) {
                        TopicPartition topicPartition = topicPartitionIterator.next();
                        
                        logger.info("CurrentOffset=" + kafkaConsumer.position(topicPartition));
                        logger.info("Committed Offset=" + kafkaConsumer.committed(topicPartition));
                        switch(startingOffset) {
	                        case 0:
	                        	logger.info("Setting Offset to Beginning");
	                        	kafkaConsumer.seekToBeginning(partitions);
	                        	break;
	                        case -1:
	                        	logger.info("Setting Offset to End");
	                        	kafkaConsumer.seekToEnd(partitions);
	                        	break;
	                        case -2:
	                        	break;
	                        default:
	                        	logger.info("Setting To Offset=" + startingOffset);
	                        	kafkaConsumer.seek(topicPartition, startingOffset);
                        }
                    }
                }
            });
            
            // Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(record.offset() + "=>" + record.key() + " "  + record.value());
                    }
                    if (startingOffset == -2) {
                    	logger.info("=== Offset CommitSync");
                        kafkaConsumer.commitSync();
                    }
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}
