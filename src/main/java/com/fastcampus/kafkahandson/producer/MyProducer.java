package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Component
@RequiredArgsConstructor
public class MyProducer {

    ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(MyMessage myMessage) throws JsonProcessingException {
        kafkaTemplate.send(MY_JSON_TOPIC, String.valueOf(myMessage.getAge()), objectMapper.writeValueAsString(myMessage));
    }
}
