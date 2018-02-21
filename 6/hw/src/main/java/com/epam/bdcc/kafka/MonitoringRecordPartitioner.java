package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    // 'Taken' from DefaultPartitioner. Used to prevent 'negative' partition values.
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // Get partition by platform-independent key hashcode -
        // to ensure records with same Key will always end up on same partition.
        if (value instanceof MonitoringRecord && key != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            return toPositive(((String) key).hashCode()) % numPartitions;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
