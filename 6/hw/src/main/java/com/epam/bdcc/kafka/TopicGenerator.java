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
import java.util.stream.Stream;

public class TopicGenerator implements GlobalConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    /**
     * Serves as an entry point
     *
     * @param args input args.
     */
    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean.parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final String  sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String  topicName  = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();

            // Read file 'sampleFile' var points to and push extracted records to Kafka raw topic.
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
                                producer.send(record).get();
                            } catch (Exception e) {
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
