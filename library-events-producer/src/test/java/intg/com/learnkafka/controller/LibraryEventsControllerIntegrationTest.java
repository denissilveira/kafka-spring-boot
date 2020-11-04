package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 1)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @DisplayName("on postLibraryEvent, with{valid data}, create new book")
    @Timeout(5)
    void postLibraryEvent() throws InterruptedException {

        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        var headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        var responseEntity = restTemplate.exchange("/v1/libraryevents", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        var value = consumerRecord.value();
        assertNotNull(value);
        assertTrue(value.contains("DSilveira"));
    }

    @Test
    @DisplayName("on putLibraryEvent, with{valid data}, update the book")
    @Timeout(5)
    void putLibraryEvent() throws InterruptedException {

        var book = buildValidBook();

        var libraryEvent = buildValidLibraryEvent(book);
        libraryEvent.setLibraryEventId(123);
        var headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        var request = new HttpEntity<>(libraryEvent, headers);
        var responseEntity = restTemplate.exchange("/v1/libraryevents", HttpMethod.PUT, request, LibraryEvent.class);

        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        var value = consumerRecord.value();
        assertNotNull(value);
        assertTrue(value.contains("DSilveira"));

    }

    private Book buildValidBook() {
        return Book.builder()
            .bookId(123)
            .bookAuthor("DSilveira")
            .bookName("Kafka Using Spring Boot")
            .build();
    }

    private LibraryEvent buildValidLibraryEvent(Book book) {
        return LibraryEvent.builder()
            .libraryEventId(null)
            .book(book)
            .build();
    }
}
