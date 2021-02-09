package com.mc.reactivekafkasample.kafka.initializers;

import com.mc.reactivekafkasample.Message;
import com.mc.reactivekafkasample.kafka.ReactiveKafkaConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@Profile("!test")
@RequiredArgsConstructor
class MessageTopicConsumerInitializer {

    private final ReactiveKafkaConsumer<Message> reactiveKafkaConsumer;

    @PostConstruct
    public void startConsuming() {
        reactiveKafkaConsumer.provide()
                             .subscribe();
    }
}
