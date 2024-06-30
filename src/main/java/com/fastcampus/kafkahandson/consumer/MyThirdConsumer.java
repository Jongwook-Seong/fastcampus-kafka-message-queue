package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Component
public class MyThirdConsumer {

    @KafkaListener(topics = { MY_JSON_TOPIC }, groupId = "batch-test-consumer-group", containerFactory = "batchKafkaListenerContainerFactory")
    public void accept(List<ConsumerRecord<String, MyMessage>> messages) {
        System.out.println("[Third Consumer] Message arrived! - count " + messages.size());
        messages.forEach(message -> {
            System.out.println("ㄴ [Third Consumer] Value " + message.value() +
                    " | Offset " + message.offset() + " | Partition " + message.partition());
        });
    }
}
