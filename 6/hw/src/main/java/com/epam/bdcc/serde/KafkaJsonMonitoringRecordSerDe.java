package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {

        byte[] output = null;
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            output = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }

        return output;
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {

        MonitoringRecord record = null;
        ObjectMapper mapper = new ObjectMapper();

        try {
            record = mapper.readValue(data, MonitoringRecord.class);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }

        return record;
    }

    @Override
    public void close() {
    }
}
