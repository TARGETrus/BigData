package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    // 'Taken' from DefaultPartitioner.
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //TODO : Add implementation for MonitoringRecord Partitioner
        // Temporary implementation. Evenly distributes records with the same keys to same partition.
        if (value instanceof MonitoringRecord && keyBytes != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            return toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    @Override
    public void close() {
        //TODO :  Add implementation for close, if needed
    }

    @Override
    public void configure(Map<String, ?> map) {
        //TODO :  Add implementation for configure, if needed
    }
}
