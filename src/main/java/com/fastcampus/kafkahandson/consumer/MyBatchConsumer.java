package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Component
public class MyBatchConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

//    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @KafkaListener(topics = { MY_JSON_TOPIC }, groupId = "batch-test-consumer-group", containerFactory = "batchKafkaListenerContainerFactory", concurrency = "3")
    public void accept(List<ConsumerRecord<String, String>> messages) {
        System.out.println("[Batch Consumer] Batch message arrived! - count " + messages.size());
//        messages.forEach(message -> executorService.submit(() -> {
        messages.forEach(message -> {
            MyMessage myMessage;
            try {
                myMessage = objectMapper.readValue(message.value(), MyMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(1000);
                System.out.println("ㄴ [Batch Consumer(" + Thread.currentThread().getId()
                        + ")] [Partition - " + message.partition() + " | Offset - " + message.offset() + "] " + myMessage);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
