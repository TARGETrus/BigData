package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class TopicGenerator implements GlobalConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean.parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long    batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG)); // ????
            final int     batchSize  = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));          // ????
            final String  sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String  topicName  = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();

            // Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.
            try (Stream<String> stream = Files.lines(Paths.get(sampleFile))) {

                // Skip one line if 'skipHeader' param is set to true.
                stream
                        .skip(skipHeader ? 1 : 0)
                        .forEach(line -> {
                            try {
                                String[] values = line.split("[\\s]*,[\\s]*");
                                MonitoringRecord rawRecord = new MonitoringRecord(values);
                                ProducerRecord<String, MonitoringRecord> record =
                                        new ProducerRecord<>(topicName, KafkaHelper.getKey(rawRecord), rawRecord);
                                RecordMetadata metadata = producer.send(record).get();
                                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                                        record.key(), record.value(), metadata.partition(), metadata.offset());
                            } catch (/*InterruptedException | Execution*/Exception e) {
                                e.printStackTrace();
                                LOGGER.error(e.getMessage(), e);
                            }
                        });
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.error(e.getMessage(), e);
            } finally {
                producer.close();
                LOGGER.info("Called producer.close().");
            }
        }
    }
}
