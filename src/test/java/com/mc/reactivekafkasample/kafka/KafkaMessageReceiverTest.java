package com.mc.reactivekafkasample.kafka;

import com.mc.reactivekafkasample.Message;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

@Slf4j
@SpringBootTest(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "debug=true"})
@ActiveProfiles("kafka-test")
@EmbeddedKafka
@DirtiesContext
public class KafkaMessageReceiverTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    @Autowired
    private ReactiveKafkaConsumer<Message> reactiveKafkaConsumer;

    @Test
    public void testKafkaReceiver() {
        // given
        var expectedMessage = Message.builder().id(UUID.randomUUID().toString()).msg("Hello kafka").build();

        // except
        StepVerifier
                .create(reactiveKafkaConsumer.provide())
                .thenAwait(Duration.ofSeconds(5))
                .then(() -> sendMessage(expectedMessage))
                .expectNextMatches(record -> record.value().equals(expectedMessage))
                .thenCancel()
                .verify(TEST_TIMEOUT);
    }

    @SneakyThrows
    private void sendMessage(Object value) {
        var json = KafkaJsonDeserializer.OBJECT_MAPPER.writeValueAsString(value);
        getKafkaTemplate().send("messageTopic", UUID.randomUUID().toString(), json);
    }

    private KafkaTemplate<String, String> getKafkaTemplate() {
        var producerProps = KafkaTestUtils.producerProps(embeddedKafka.getBrokersAsString());
        var producerFactory = new DefaultKafkaProducerFactory<String, String>(producerProps);
        producerFactory.setKeySerializer(new StringSerializer());
        return new KafkaTemplate<>(producerFactory);
    }

}
