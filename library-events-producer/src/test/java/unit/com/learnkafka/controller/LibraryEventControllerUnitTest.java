package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    @Test
    @DisplayName("on postLibraryEvent, with{valid data}, create new book")
    void onPostLibraryEventWithValidaDataThenCreateNewBook() throws Exception {

        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        var json = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/libraryevents")
            .content(json)
            .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());

    }

    @Test
    @DisplayName("on postLibraryEvent, with{book null}, returns 400 BAD REQUEST ")
    void onPostLibraryEventWithBookNullThenReturnsBadRequest() throws Exception {

        var libraryEvent = buildValidLibraryEvent(null);
        var json = objectMapper.writeValueAsString(libraryEvent);
        var expectedErrorMessage = "book - must not be null";

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/libraryevents")
            .content(json)
            .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
        .andExpect(content().string(expectedErrorMessage));

    }

    @Test
    @DisplayName("on putLibraryEvent, with{valid data}, update the book")
    void onPutLibraryEventWithValidaDataThenUpdateBook() throws Exception {

        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        libraryEvent.setLibraryEventId(123);
        var json = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(
            put("/v1/libraryevents")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    }

    @Test
    @DisplayName("on putLibraryEvent, with{libraryEventId null}, returns 400 BAD REQUEST ")
    void onPutLibraryEventWithLibraryEventIdNullThenReturnsBadRequest() throws Exception {

        var book = buildValidBook();
        var libraryEvent = buildValidLibraryEvent(book);
        var json = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(
            put("/v1/libraryevents")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isBadRequest())
        .andExpect(content().string("Please pass the LibraryEventId"));
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
