package com.fastcampus.kafkahandson.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.fastcampus.kafkahandson.model.Topic.MY_CUSTOM_CDC_TOPIC_DLT;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Bean
    @Primary
//    @ConfigurationProperties("spring.kafka.json")
    @ConfigurationProperties("spring.kafka")
    public KafkaProperties kafkaProperties() {
        return new KafkaProperties();
    }

    /** 컨슈머 관련 설정 **/
    @Bean
    @Primary
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getValueDeserializer());
//        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 수동커밋 설정 (default: 자동커밋)
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
//            CommonErrorHandler errorHandler,
            KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(generatedBackOff());
//        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
//        factory.setCommonErrorHandler(errorHandler);
//        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
//        factory.setCommonErrorHandler(errorHandler);
//        factory.setCommonErrorHandler(new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), generateBackOff()));
        factory.setCommonErrorHandler(new DefaultErrorHandler((record, exception) -> {
            kafkaTemplate.send(MY_CUSTOM_CDC_TOPIC_DLT, (String) record.key(), record.value());
            /* 내가 원하는 로직 들어갈 곳 */
            System.out.println("Give up! - " + exception.getMessage());
        }, generateBackOff()));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // 수동커밋 설정 시 추가
        factory.setConcurrency(1);
        return factory;
    }

    /** 프로듀서 관련 설정 **/
    @Bean
    @Primary
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getValueSerializer());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getProducer().getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    @Primary
    CommonErrorHandler errorHandler() {
        CommonContainerStoppingErrorHandler cseh = new CommonContainerStoppingErrorHandler();
        AtomicReference<Consumer<?, ?>> consumer2 = new AtomicReference<>();
        AtomicReference<MessageListenerContainer> container2 = new AtomicReference<>();
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, e) -> {
            cseh.handleRemaining(e, Collections.singletonList(consumerRecord), consumer2.get(), container2.get());
            // container stopping error handler를 통해서 해당 컨테이너(컨슈머)를 중지시킨다.
        }, generateBackOff()) {
            @Override
            public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
                consumer2.set(consumer);
                container2.set(container);
                super.handleRemaining(thrownException, records, consumer, container);
            }
        };
//        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }

    @Bean
    @Primary
    public KafkaTemplate<String, ?> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(producerFactory(kafkaProperties));
    }

    /** 배치 컨슈머 설정 **/
    @Bean
    @Qualifier("batchConsumerFactory")
    public ConsumerFactory<String, Object> batchConsumerFactory(KafkaProperties secondKafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, secondKafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, secondKafkaProperties.getConsumer().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, secondKafkaProperties.getConsumer().getValueDeserializer());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @Qualifier("batchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> batchKafkaListenerContainerFactory(
            @Qualifier("batchConsumerFactory") ConsumerFactory<String, Object> batchConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(1);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private BackOff generateBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2L); // 1000ms 간격으로 시작해서 2배씩 증가
        backOff.setMaxElapsedTime(10000L); // 최대 10000ms까지만 증가
        return backOff;
    }
}
