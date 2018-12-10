package com.rueggerllc.kafka.stream;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.log4j.Logger;


public class SensorConsumer {
	
	private static final Logger logger = Logger.getLogger(SensorConsumer.class);
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
	
	public void getWordCounts(String topicName, String groupName) throws Exception {
		try {
			
	        logger.info("============== WordCountConsumer BEGIN ==============");
	        Properties props = new Properties();
	        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
	        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
	        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
	        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

	        
	        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
	        // Note: To re-run the demo, you need to use the offset reset tool:
	        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	        StreamsBuilder builder = new StreamsBuilder();
	        KStream<String, String> source = builder.stream("streams-plaintext-input");
	        
	        logger.info("===== Builder and Source BUILT====");

	        KTable<String, Long> counts = source
	            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
	                @Override
	                public Iterable<String> apply(String value) {
	                	logger.info("BUILDING FLATMAP");
	                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
	                }
	            })
	            .groupBy(new KeyValueMapper<String, String, String>() {
	                @Override
	                public String apply(String key, String value) {
	                	System.out.println("GROUP BY CALLED FOR KEY=" + key + " VALUE=" + value);
	                    return value;
	                }
	            })
	            .count();

	        // Need to override value serde to Long type
	        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
	        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
	        final CountDownLatch latch = new CountDownLatch(1);

	        // attach shutdown handler to catch control-c
	        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
	            @Override
	            public void run() {
	                streams.close();
	                latch.countDown();
	            }
	        });

	        try {
	            streams.start();
	            latch.await();
	        } catch (Throwable e) {
	            System.exit(1);
	        }
	        System.exit(0);
	        
	        
			
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
	            logger.error(String.format("Usage: %s <topicName> <groupId>\n", SensorConsumer.class.getSimpleName()));
	            System.exit(-1);
	        }
	        String topicName = argv[0];
	        String groupName = argv[1]; 
	        
	        SensorConsumer consumer = new SensorConsumer();
	        // consumer.execute(topicName, groupName);
	        consumer.getWordCounts(topicName, groupName);
	       
    	} catch (Exception e) {
    		logger.error("Error", e);
    	}

    }

}

