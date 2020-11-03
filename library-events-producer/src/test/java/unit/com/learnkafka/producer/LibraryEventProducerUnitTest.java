package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
    @DisplayName("on sendLibraryEvent, with{failure}, throws Exception ")
    void onSendLibraryEventWithFailureThenThrowsException() {
        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        var future = new SettableListenableFuture();

        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        assertThrows(Exception.class, ()->eventProducer.sendLibraryEvent(libraryEvent).get());
    }

    @Test
    @DisplayName("on sendLibraryEvent, with{success}, create new message")
    void onSendLibraryEventWithSuccessThenCreateNewMessage()
        throws JsonProcessingException, ExecutionException, InterruptedException {

        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        var record = objectMapper.writeValueAsString(libraryEvent);
        var future = new SettableListenableFuture();

        var producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),record );
        var recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        var sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);

        future.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        var listenableFuture =  eventProducer.sendLibraryEvent(libraryEvent);
        var sendResult1 = listenableFuture.get();
        assertEquals(1, sendResult1.getRecordMetadata().partition());

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
