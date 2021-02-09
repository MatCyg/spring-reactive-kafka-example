package com.mc.reactivekafkasample.kafka;

import com.mc.reactivekafkasample.Message;
import com.mc.reactivekafkasample.MessageDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private final KafkaProperties kafkaProperties;

    @Bean("messageTopicConsumer")
    public ReactiveKafkaConsumer<Message> kafkaFooTopicConsumer() {
        var topicName = "messageTopic";
        return new ReactiveKafkaConsumer<>(createKafkaReceiver(MessageDeserializer.class, topicName));
    }

    private <T> KafkaReceiver<String, T> createKafkaReceiver(Class<? extends Deserializer<T>> deserializer, String topicName) {
        var receiverOptions = ReceiverOptions.<String, T>create().subscription(List.of(topicName))
                .consumerProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers())
                .consumerProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .consumerProperty(ENABLE_AUTO_COMMIT_CONFIG, false)
                .consumerProperty(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
                .consumerProperty(CLIENT_ID_CONFIG, topicName + "_" + UUID.randomUUID())
                .consumerProperty(GROUP_ID_CONFIG, "reactive-kafka-sample");
        return KafkaReceiver.create(receiverOptions);
    }
}
