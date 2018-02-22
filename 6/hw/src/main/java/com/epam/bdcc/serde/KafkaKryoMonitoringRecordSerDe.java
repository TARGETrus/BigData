package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;

/**
 * Kafka kry SerDe.
 * Using internal kryo serializer class for serialization inside overridden kafka serializer/deserializer methods.
 */
public class KafkaKryoMonitoringRecordSerDe implements Closeable, AutoCloseable,  Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKryoMonitoringRecordSerDe.class);

    private ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.addDefaultSerializer(MonitoringRecord.class, new KryoInternalSerializer());
        return kryo;
    });

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord record) {

        ByteBufferOutput output = new ByteBufferOutput(4096);
        kryos.get().writeObject(output, record);

        return output.toBytes();
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), MonitoringRecord.class);
        } catch(Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void close() {
    }

    /**
     * Internal class used for Kryo SerDe.
     */
    public static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<MonitoringRecord> {

        @Override
        public void write(Kryo kryo, Output kryoOutput, MonitoringRecord monitoringRecord) {
            kryoOutput.writeString(monitoringRecord.getStateCode());
            kryoOutput.writeString(monitoringRecord.getCountyCode());
            kryoOutput.writeString(monitoringRecord.getSiteNum());
            kryoOutput.writeString(monitoringRecord.getParameterCode());
            kryoOutput.writeString(monitoringRecord.getPoc());
            kryoOutput.writeString(monitoringRecord.getLatitude());
            kryoOutput.writeString(monitoringRecord.getLongitude());
            kryoOutput.writeString(monitoringRecord.getDatum());
            kryoOutput.writeString(monitoringRecord.getParameterName());
            kryoOutput.writeString(monitoringRecord.getDateLocal());
            kryoOutput.writeString(monitoringRecord.getTimeLocal());
            kryoOutput.writeString(monitoringRecord.getDateGMT());
            kryoOutput.writeString(monitoringRecord.getTimeGMT());
            kryoOutput.writeString(monitoringRecord.getSampleMeasurement());
            kryoOutput.writeString(monitoringRecord.getUnitsOfMeasure());
            kryoOutput.writeString(monitoringRecord.getMdl());
            kryoOutput.writeString(monitoringRecord.getUncertainty());
            kryoOutput.writeString(monitoringRecord.getQualifier());
            kryoOutput.writeString(monitoringRecord.getMethodType());
            kryoOutput.writeString(monitoringRecord.getMethodCode());
            kryoOutput.writeString(monitoringRecord.getMethodName());
            kryoOutput.writeString(monitoringRecord.getStateName());
            kryoOutput.writeString(monitoringRecord.getCountyName());
            kryoOutput.writeString(monitoringRecord.getDateOfLastChange());
            kryoOutput.writeDouble(monitoringRecord.getPrediction());
            kryoOutput.writeDouble(monitoringRecord.getError());
            kryoOutput.writeDouble(monitoringRecord.getAnomaly());
            kryoOutput.writeDouble(monitoringRecord.getPredictionNext());
        }

        @Override
        public MonitoringRecord read(Kryo kryo, Input kryoInput, Class<MonitoringRecord> type) {

            return new MonitoringRecord(kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readString(), kryoInput.readString(), kryoInput.readString(),
                    kryoInput.readString(), kryoInput.readDouble(), kryoInput.readDouble(), kryoInput.readDouble(),
                    kryoInput.readDouble());
        }
    }
}
