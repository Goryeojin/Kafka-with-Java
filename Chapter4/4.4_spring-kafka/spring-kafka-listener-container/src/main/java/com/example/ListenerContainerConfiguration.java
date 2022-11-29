package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ListenerContainerConfiguration {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> customContainerFactory() {
        // KafkaListenerContainerFactory Bean 객체를 리턴하는 메서드 작성.

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 카프카 컨슈머 실행 시 필요햔 옵션값 선언. group.id 는 리스너 컨테이너에도 선언하므로 생략 가능.

        DefaultKafkaConsumerFactory cf = new DefaultKafkaConsumerFactory<>(props);
        // 컨슈머 옵션값을 파라미터로 받는 DefaultKafkaConsumerFactory 인스턴스 생성.
        // => 리스너 컨테이너 팩토리 생성할 때 컨슈머 기본 옵션을 설정하는 용도.
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        // ConcurrentKafkaListenerContainerFactory => 리스너 컨테이너를 만들기 위해 사용.
        // 2개 이상의 컨슈머 리스너를 만들 때 사용되며 concurrency 를 1로 설정할 경우 1개 컨슈머 스레드로 실행됨.
        factory.getContainerProperties().setConsumerRebalanceListener(
                new ConsumerAwareRebalanceListener() {
                    // 리밸런스 리스너를 선언하기 위해 setConsumerRebalanceListener 메서드 호출.
                    // => 스프링 카프카에서 제공하는 메서드로 기존에 사용되는 카프카 컨슈머 리밸런스 리스너에 2개의 메서드 호출.
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // 커밋 되기 전 리밸런스가 발생했을 때
            }

            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // 커밋 이후에 리밸런스가 발생했을 때
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
        });
        factory.setBatchListener(false);
        // 레코드 리스너를 사용함을 명시하기 위해 setBatchListener() 에 false 를 파라미터로 넣는다.
        // 배치로 사용하고 싶다면 true 로 설정.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // AckMode 설정. 레코드 단위로 커밋하기 위해 RECORD 로 설정함.
        factory.setConsumerFactory(cf);
        // 컨슈머 설정값을 가지고 있는 DefaultKafkaConsumerFactory 인스턴스를 ConcurrentKafkaListenerContainerFactory 의 컨슈머 팩토리에 설정.
        return factory;
    }
}
