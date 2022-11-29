package com.example.springkafkatemplateproducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
// KafkaTemplate 빈 객체를 등록하기 위해 Configuration 어노테이션을 선언한 클래스를 만든다.
// 이 클래스에서 KafkaTemplate 빈 객체가 등록된다.
public class KafkaTemplateConfiguration {

    @Bean
    // KafkaTemplate 빈 객체를 리턴하는 빈 객체다. 메서드 이름인 customKafkaTemplate 로 빈 객체가 생성된다.
    public KafkaTemplate<String, String> customKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // ProducerFactory 를 사용하여 KafkaTemplate 객체를 만들 때는 프로듀서 필수 옵션을 직접 넣는다.

        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
        // KafkaTemplate 객체를 만들기 위한 ProducerFactory 초기화

        return new KafkaTemplate<>(pf);
        // 빈 객체로 사용할 KafkaTemplate 인스턴스 초기화 후 리턴.
        // 스프링 카프카에서는 KafkaTemplate, ReplyingKafkaTemplate, RoutingKafkaTemplate 제공.
        // ReplyingKafkaTemplate : 컨슈머가 특정 데이터를 전달받았는지 여부 확인
        // RoutingKafkaTemplate : 전송하는 토픽별로 옵션을 다르게 설정 가능
    }
}
