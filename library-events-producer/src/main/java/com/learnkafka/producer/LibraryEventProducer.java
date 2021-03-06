package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    private static final String TOPIC = "library-events"; // create it in a properties.

    @Autowired
    private KafkaProducer<Integer, String> libraryEventsProducer;
    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Transactional
    public void sendLibraryEventIdempotence(LibraryEvent libraryEvent)
        throws JsonProcessingException {

        libraryEventsProducer.initTransactions();

        try {
            libraryEventsProducer.beginTransaction();
            var key = libraryEvent.getLibraryEventId();
            var value = objectMapper.writeValueAsString(libraryEvent);
            var producerRecord = buildProducerRecord(key, value, TOPIC);
            var listenableFuture =  libraryEventsProducer.send(producerRecord);
            libraryEventsProducer.commitTransaction();
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}", e.getMessage());
            libraryEventsProducer.abortTransaction();
            throw e;
        }
    }


    public void sendDefaultLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.getLibraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var listenableFuture =  kafkaTemplate.sendDefault(key,value);
        addCallback(listenableFuture, key, value);
    }

    @Transactional
    public ListenableFuture<SendResult<Integer,String>> sendLibraryEvent(LibraryEvent libraryEvent)
        throws JsonProcessingException {

        var key = libraryEvent.getLibraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var producerRecord = buildProducerRecord(key, value, TOPIC);
        var listenableFuture =  kafkaTemplate.send(producerRecord);
        addCallback(listenableFuture, key, value);

        return listenableFuture;
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
        throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        try {
            var key = libraryEvent.getLibraryEventId();
            var value = objectMapper.writeValueAsString(libraryEvent);
            return kafkaTemplate.sendDefault(key,value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}", e.getMessage());
            throw e;
        }

    }

    private void addCallback(ListenableFuture listenableFuture, Integer key, String value) {

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }
}
