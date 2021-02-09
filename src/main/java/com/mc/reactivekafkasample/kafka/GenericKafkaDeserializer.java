package com.mc.reactivekafkasample.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface GenericKafkaDeserializer<T> extends Deserializer<T> {

    @Override
    default void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    default void close() {
    }

    @Override
    default T deserialize(String topic, byte[] data) {
        return deserialize(data);
    }

    default T deserialize(byte[] data) {
        try {
            return KafkaJsonDeserializer.OBJECT_MAPPER.readValue(data, getTypeReference());
        } catch (IOException e) {
            var inputData = new String(data, UTF_8);
            var errorMsg = String.format("Failed to deserialize %s object: %s", getTypeReference().getType(), inputData);
            throw new RuntimeException(errorMsg, e);
        }
    }

    TypeReference<T> getTypeReference();
}

