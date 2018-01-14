package com.rueggerllc.kafka.partition;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


public class Producer {
    private static Scanner in;
    private static final Logger logger = Logger.getLogger(Producer.class);
    
    private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092";
    
    public static void main(String[] argv)throws Exception {
        if (argv.length != 1) {
            logger.error("Please specify parm0=topic ");
            System.exit(-1);
        }
        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        // Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());
        configProperties.put("partitions.0","USA");
        configProperties.put("partitions.1","India");

        org.apache.kafka.clients.producer.Producer<String,String> producer = new KafkaProducer(configProperties);
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);

            producer.send(rec, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    logger.info("Message sent to topic ->" + metadata.topic() + 
                    		    " partition->" + metadata.partition() +
                    		    " stored at offset->" + metadata.offset());
                }
            });
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}
