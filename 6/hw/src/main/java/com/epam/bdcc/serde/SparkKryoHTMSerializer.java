package com.epam.bdcc.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMSerializer.class);

    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    public T copy(Kryo kryo, T original) {
        // Implement custom clone if needed.
        return super.copy(kryo, original);
    }

    @Override
    public void write(Kryo kryo, Output kryoOutput, T t) {
        try {
            HTMObjectOutput writer = htmSerializer.getObjectOutput(kryoOutput);
            writer.writeObject(t, t.getClass());
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public T read(Kryo kryo, Input kryoInput, Class<T> aClass) {
        try {
            HTMObjectInput reader = htmSerializer.getObjectInput(kryoInput);
            return aClass.cast(reader.readObject(aClass));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(Network.class, new SparkKryoHTMSerializer<>());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkKryoHTMSerializer<>());
    }
}
