package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.model.MyMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Component
@RequiredArgsConstructor
public class MyProducer {

    private final KafkaTemplate<String, MyMessage> kafkaTemplate;

    public void sendMessage(MyMessage myMessage) {
        kafkaTemplate.send(MY_JSON_TOPIC, String.valueOf(myMessage.getAge()), myMessage);
    }
}
