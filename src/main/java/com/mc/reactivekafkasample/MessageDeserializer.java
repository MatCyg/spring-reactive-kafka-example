package com.mc.reactivekafkasample;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mc.reactivekafkasample.kafka.GenericKafkaDeserializer;

public class MessageDeserializer implements GenericKafkaDeserializer<Message> {
    private static final TypeReference<Message> TYPE_REFERENCE = new TypeReference<>() {
    };

    @Override
    public TypeReference<Message> getTypeReference() {
        return TYPE_REFERENCE;
    }
}