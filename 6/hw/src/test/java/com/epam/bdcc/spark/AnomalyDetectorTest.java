package com.epam.bdcc.spark;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.serde.KafkaJsonMonitoringRecordSerDe;
import com.google.common.io.Files;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.*;

import java.io.File;
import java.util.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Ignore
public class AnomalyDetectorTest {

    private static JavaStreamingContext ssc;
    private static File outputFolder;
    private static KafkaTestUtils kafkaTestUtils;

    private static final String TOPIC_ONE = "topic_1";
    private static final String AUTO_OFFSET_RESET = "earliest";

    // Use this to share expensive JavaStreamingContext initialization.
    @BeforeClass
    public static void beforeTests() {
        ssc = new JavaStreamingContext("local[4]", "realtime-anomaly-detection test", new Duration(60000));
        kafkaTestUtils = new KafkaTestUtils();
        kafkaTestUtils.setup();
    }

    @Before
    public void setUp() {
        outputFolder = Files.createTempDir();
        outputFolder.deleteOnExit();
    }

    // Useless. Mocks won't provide desired behaviour, embedded kafka/zk or integration tests needed.
    // No stable embedded kafka/zk found for java API.
    @Test
    public void testRun() {

        Collection<String> topics = Arrays.asList(TOPIC_ONE);
        createTopicAndSendData(TOPIC_ONE);

        Random random = new Random();

        // Create test properties for kafka test JavaInputDStream.
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(BOOTSTRAP_SERVERS_CONFIG, kafkaTestUtils.brokerAddress());
        kafkaParams.put(AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        kafkaParams.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class);
        kafkaParams.put(GROUP_ID_CONFIG,
                "java-test-consumer-" + random.nextInt() + "-" + System.currentTimeMillis());

        JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> dStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, MonitoringRecord>Subscribe(topics, kafkaParams)
                );

        // AnomalyDetector.processData(dStream, TOPIC_ONE);

    }

    private void createTopicAndSendData(String topic) {
        String[] messages = {"{stateCode:10,countyCode:001,siteNum:0002,parameterCode:44201,poc:1,latitude:38.986672,longitude:-75.5568,datum:WGS84,parameterName:Ozone,dateLocal:2014-01-07,timeLocal:00:00,dateGMT:2014-01-07,timeGMT:05:00,sampleMeasurement:0.039,unitsOfMeasure:Parts per million,mdl:0.005,uncertainty:,qualifier:,methodType:FEM,methodCode:047,methodName:INSTRUMENTAL - ULTRA VIOLET,stateName:Delaware,countyName:Kent,dateOfLastChange:2014-02-12,prediction:0.0,error:0.0,anomaly:0.0,predictionNext:0.0}"};
        kafkaTestUtils.createTopic(topic);
        kafkaTestUtils.sendMessages(topic, messages);
    }

    @After
    public void tearDown() {
    }

    @AfterClass
    public static void afterTests() {
        if (ssc != null) {
            ssc.stop();
        }
        if (kafkaTestUtils != null) {
            kafkaTestUtils.teardown();
        }
    }
}
