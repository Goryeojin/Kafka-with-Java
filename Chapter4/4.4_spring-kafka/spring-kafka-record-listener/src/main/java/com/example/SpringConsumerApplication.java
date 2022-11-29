package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class SpringConsumerApplication {

    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringConsumerApplication.class, args);
    }

    @KafkaListener(topics = "test", groupId = "test-group-00")
    public void recordListener(ConsumerRecord<String, String> record) {
    // 가장 기본적인 리스너 선언. 어노테이션 옵션으로 topics, groupId 지정
    // poll() 호출 시 가져온 레코드들은 차례대로 개별 레코드의 메시지 값을 파라미터로 받게 됨.
    // 파라미터로 ConsumerRecord 를 받기 때문에 메시지 키, 메시지 값에 대한 처리를 메서드 안에서 수행한다.
        logger.info(record.toString());
    }

    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void singleTopicListener(String messageValue) {
        // 메시지 값을 파라미터로 받는 리스너.
        // 스프링 카프카의 역직렬화 클래스 기본값 StringDeserializer 사용하여 String 형으로 메시지 값을 전달받는다.
        logger.info(messageValue);
    }

    @KafkaListener(topics = "test", groupId = "test-group-02",
                    properties = {
                        "max.poll.interval.ms:60000",
                        "auto.offset.reset:earliest"
                    })
    // 개별 리스너에 카프카 컨슈머 옵션값을 부여하고 싶다면 KafkaListener 어노테이션의 properties 옵션 사용.
    public void singleTopicWithPropertiesListener(String messageValue) {
        logger.info(messageValue);
    }

    @KafkaListener(topics = "test", groupId = "test-group-03", concurrency = "3")
    // 2개 이상의 카프카 컨슈머 스레드를 실행하고 싶다면 concurrency 옵션을 사용해 병렬처리한다.
    // 파티션이 3개라면 concurrency 를 3으로 설정해 3개 파티션에 3개 컨슈머 스레드가 각각 할당되어 병렬처리량이 늘어난다.
    public void concurrentTopicListener(String messageValue) {
        logger.info(messageValue);
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "test01", partitions = {"0", "1"}),
            @TopicPartition(topic = "test02", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
            }, groupId = "test-group-04")
    // 특정 토픽의 특정 파티션만 구독하고 싶다면 topicPartitions 파라미터를 사용한다.
    // 추가로 PartitionOffset 어노테이션을 활용해 특정 파티션의 특정 오프셋까지 지정할 수 있다.
    // 이 경우에는 그룹 아이디에 관계없이 항상 설정한 오프셋의 데이터부터 가져온다.
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        logger.info(record.toString());
    }
}
