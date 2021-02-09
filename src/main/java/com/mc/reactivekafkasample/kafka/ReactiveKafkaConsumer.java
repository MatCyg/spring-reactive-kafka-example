package com.mc.reactivekafkasample.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@RequiredArgsConstructor
public class ReactiveKafkaConsumer<T> {

    private final KafkaReceiver<String, T> kafkaReceiver;

    public Flux<ReceiverRecord<String, T>> provide() {
        return kafkaReceiver
                .receive()
                .onErrorContinue(this::logError)
                .doOnNext(this::logConsumerRecordReceived)
                .doOnNext(this::acknowledge);
        // in the past I used reactor.core.publisher.TopicProcessor for retry and backoff, but
        // the implementation have changed https://github.com/reactor/reactor-core/issues/2431
    }

    private void logError(Throwable throwable, Object o) {
        log.error("Failed to receive kafka event, object={}", o, throwable);
    }

    private void logConsumerRecordReceived(ConsumerRecord<String, T> consumerRecord) {
        log.debug("Received event from kafka topic, id: {}, content: {}", consumerRecord.key(),
                consumerRecord.value());
    }

    private void acknowledge(ReceiverRecord<String, T> receiverRecord) {
        receiverRecord.receiverOffset().acknowledge();
    }

}
