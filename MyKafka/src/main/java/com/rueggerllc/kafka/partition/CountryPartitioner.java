package com.rueggerllc.kafka.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

public class CountryPartitioner implements Partitioner {
	
    private static Map<String,Integer> countryToPartitionMap;
    private static final Logger logger = Logger.getLogger(CountryPartitioner.class);

    // This method will gets called at the start, you should use it to do one time startup activity
    public void configure(Map<String, ?> configs) {
        System.out.println("Inside CountryPartitioner.configure " + configs);
        countryToPartitionMap = new HashMap<String, Integer>();
        for(Map.Entry<String,?> entry: configs.entrySet()){
            if(entry.getKey().startsWith("partitions.")){
                String keyName = entry.getKey();
                String value = (String)entry.getValue();
                System.out.println( keyName.substring(11));
                int partitionID = Integer.parseInt(keyName.substring(11));
                countryToPartitionMap.put(value, partitionID);
            }
        }
    }

    // This method will get called once for each message
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        for (PartitionInfo partitionInfo : partitions) {
        	logger.info("NEXT PARTITION ID=" + partitionInfo.partition());
        }
        
        String valueStr = (String)value;
        String countryName = ((String) value).split(":")[0];
        if (countryToPartitionMap.containsKey(countryName)) {
            // If the country is mapped to particular partition return it
            return countryToPartitionMap.get(countryName);
        } else {
        	logger.info("Country Not Specified, compute partitionID");
            // If no country is mapped to particular partition distribute between remaining partitions
            int noOfPartitions = cluster.topics().size();
            int partitionID = value.hashCode()%noOfPartitions + countryToPartitionMap.size();
            logger.info("Sending to Partition:" + partitionID);
            return partitionID;
        }
    }

    // This method will get called at the end and gives your partitioner class chance to cleanup
    public void close() {}
}
