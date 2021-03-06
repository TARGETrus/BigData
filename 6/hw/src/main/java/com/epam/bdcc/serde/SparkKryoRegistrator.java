package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class SparkKryoRegistrator implements KryoRegistrator {

    public SparkKryoRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {

        // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
        // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        SparkKryoHTMSerializer.registerSerializers(kryo);

        kryo.register(MonitoringRecord.class, new KafkaKryoMonitoringRecordSerDe.KryoInternalSerializer());
        kryo.register(ResultState.class);
    }
}
