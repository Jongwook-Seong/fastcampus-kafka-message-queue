package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyCdcMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_CDC_TOPIC;

@Component
public class MyCdcConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = { MY_CDC_TOPIC }, groupId = "cdc-consumer-group", concurrency = "1")
    public void listen(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) throws JsonProcessingException {
        MyCdcMessage myCdcMessage = objectMapper.readValue(message.value(), MyCdcMessage.class);
        System.out.println("[Cdc Consumer] Message Arrived! - " + myCdcMessage.getPayload());
        acknowledgment.acknowledge();
    }
}
