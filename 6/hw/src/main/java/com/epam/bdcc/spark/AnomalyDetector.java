package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.kafka.KafkaHelper;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import scala.Tuple2;

import java.util.*;

public class AnomalyDetector implements GlobalConstants {
    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {

        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();

        if (!applicationProperties.isEmpty()) {
            final String   appName              = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String   rawTopicName         = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String   enrichedTopicName    = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String   checkpointDir        = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final String   sparkSerializer      = applicationProperties.getProperty(SPARK_INTERNAL_SERIALIZER_CONFIG);
            final String   sparkKryoRegistrator = applicationProperties.getProperty(SPARK_KRYO_REGISTRATOR_CONFIG);
            final String   sparkKryoRegRequired = applicationProperties.getProperty(SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG);
            final String   sparkBackPressure    = applicationProperties.getProperty(SPARK_BACKPRESSURE_CONFIG);
            final Duration batchDuration        = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval   = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));

            SparkConf conf = new SparkConf().setAppName(appName)
                    .set(SPARK_INTERNAL_SERIALIZER_CONFIG, sparkSerializer)
                    .set(SPARK_KRYO_REGISTRATOR_CONFIG, sparkKryoRegistrator)
                    .set(SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, sparkKryoRegRequired)
                    .set(SPARK_BACKPRESSURE_CONFIG, sparkBackPressure)
                    .set(SPARK_BACKPRESSURE_INITIAL_RATE_CONFIG, "100")
                    .set(SPARK_KAFKA_MAX_RATE_PER_PARTITION_CONFIG, "25");
            /*JavaStreamingContext jSteamingCtx = JavaStreamingContext.getOrCreate(
                    checkpointDir,
                    () -> new JavaStreamingContext(conf, batchDuration)
            );*/
            JavaStreamingContext jSteamingCtx = new JavaStreamingContext(conf, batchDuration);
            jSteamingCtx.checkpoint(checkpointDir);

            Collection<String> topics = Arrays.asList(rawTopicName, enrichedTopicName);

            JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> kafkaStream =
                KafkaUtils
                        .createDirectStream(
                                jSteamingCtx,
                                LocationStrategies.PreferConsistent(),
                                KafkaHelper.createConsumerStrategy(topics)
                        );

            kafkaStream.checkpoint(checkpointInterval);

            processData(kafkaStream, enrichedTopicName);

            jSteamingCtx.start();
            jSteamingCtx.awaitTermination();
        }
    }

    /**
     * Used to enrich MonitoringRecord's from one kafka topic using HTM functionality and write it to another.
     *
     * @param kafkaStream input stream from kafka.
     * @param topicToWriteTo kafka topic name to write output in.
     */
    public static void processData(JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> kafkaStream, String topicToWriteTo) {

        JavaPairDStream<String, MonitoringRecord> pairKafkaStream = kafkaStream
                .mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        pairKafkaStream
                .mapWithState(StateSpec.function(mappingFunc))
                .foreachRDD(rdd ->
                    rdd.foreachPartition(rddPartition -> {
                        Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                        rddPartition.forEachRemaining(rec ->{
                            ProducerRecord<String, MonitoringRecord> record =
                                    new ProducerRecord<>(topicToWriteTo, KafkaHelper.getKey(rec), rec);
                            producer.send(record);
                        });
                        producer.close();
                    })
                );
    }

    /**
     * Lambda used for stateful HTM calculations.
     * Holds one HTMNetwork stateful object for each deviceID.
     */
    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) -> {

         // case 0: timeout
         if (!recordOpt.isPresent())
             return null;

         // either new or existing device
         if (!state.exists())
             state.update(new HTMNetwork(deviceID));
         HTMNetwork htmNetwork = state.get();
         String stateDeviceID = htmNetwork.getId();
         if (!stateDeviceID.equals(deviceID))
             throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
         MonitoringRecord record = recordOpt.get();

         // get the value of DT and Measurement and pass it to the HTM
         HashMap<String, Object> m = new java.util.HashMap<>();
         m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
         m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
         ResultState rs = htmNetwork.compute(m);
         record.setPrediction(rs.getPrediction());
         record.setError(rs.getError());
         record.setAnomaly(rs.getAnomaly());
         record.setPredictionNext(rs.getPredictionNext());

         return record;
    };
}
