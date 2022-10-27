package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class NonSimpleConsumer {
    // 컨슈머 그룹에서 컨슈머가 추가 또는 제거되면 파티션을 컨슈머에 재할당하는 과정인 리밸런스가 일어난다.
    // poll() 메서드를 통해 반환받은 데이터를 모두 처리하기 전에 리밸런스가 발생하면 데이터를 중복 처리할 수 있다.
    // 리밸런스 발생 시 데이터를 중복 처리하지 않게 하기 위해서는 리밸런스 발생 시 처리한 데이터를 기준으로 커밋을 시도해야 한다.
    // 마지막으로 처리한 레코드를 기준으로 커밋을 하기 위해 리밸런스가 시작하기 직전에 커밋을 하면 된다.

    private final static Logger logger = LoggerFactory.getLogger(NonSimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static int PARTITION_NUMBER = 0;
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    private static KafkaConsumer consumer;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 리밸런스 발생 시 수동 커밋을 하기 위해 ENABLE_AUTO_COMMIT_CONFIG를 false로 설정.

        consumer = new KafkaConsumer<>(configs);
        //kafka-consumer-rebalance-listener
//        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());
        // RebalanceListener는 subscribe() 메서드에 오버라이드 변수로 포함시킨다.

        // kafka-consumer-exact-partition 파티션 할당 컨슈머
//        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        // 파티션을 직접 컨슈머에 명시적으로 할당하여 운영할 수 있다.
        // 컨슈머가 어떤 토픽, 파티션을 할당지 명시적으로 선언할 때는 assign() 메서드를 사용한다.
        // subscribe() 메서드를 사용할 때와 다르게 직접 컨슈머가 특정 토픽, 특정 파티션에 할당되므로 리밸런싱하는 과정이 없다.

        // 컨슈머에 할당된 파티션 확인 방법
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();
        // assignment() 메서드는 Set<TopicPartition> 인스턴스를 반환한다. 토픽 이름과 파티션 번호가 포함된 객체이다.
        /*
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null));
                // 레코드의 처리가 끝나면 토픽, 파티션, 오프셋에 관한 정보를 담는다.
                // 이 값은 오프셋 지정 커밋 시 사용되며 offset + 1값을 넣어야 한다.
                // 컨슈머 재시작 시에 파티션에서 가장 마지막으로 커밋된 오프셋부터 레코드를 읽기 시작하기 때문이다.
                consumer.commitSync(currentOffset);
            }
        }
         */
        // kafka-consumer-sync-offset-commit-shutdown-hook
        // 정상적으로 종료되지 않은 컨슈머는 세션 타임아웃이 발생할 때까지 컨슈머 그룹에 남는다.
        // 이로 인해 파티션의 데이터는 소모되지 못하고 컨슈머 랙이 늘어나 데이터 처리 지연이 발생한다.
        // 컨슈머를 안전하게 종료하기 위해서 KafkaConsumer 클래스는 wakeup() 메서드를 지원한다.
        // wakeup() 메서ㅕ드가 실행된 이후 poll() 메서드가 실행되면 WakeupException 예외가 발생한다.
        // 예외를 받은 이후 데이터 처리를 위한 자원들을 해제하고 close() 메서드를 호출해 컨슈머가 안전하게 종료되었음을 명시적으로 알린다.
        // 명시적으로 종료됨을 알리면 컨슈머 그룹에서 이탈되고 나머지 컨슈머들이 파티션을 할당받게 된다.
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("{}", record);
                }
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
            // 리소스 종료 처리
        } finally {
            consumer.close();
        }
    }
    private static class RebalanceListener implements ConsumerRebalanceListener {
        // 리밸런스 발생을 감지하기 위해 카프카 라이브러리는 ConsumerRebalanceListener 인터페이스를 지원한다.
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // 리밸런스가 시작되기 직전에 호출됨.
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffset);
            // 리밸런스가 발생하면 가장 마지막으로 처리 완료한 레코드를 기준으로 커밋을 실시하여 데이터 처리의 중복을 방지한다.
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // 리밸런스가 끝난 뒤에 파티션 할당이 완료되면 호출됨.
            logger.warn("partitions are assigned");
        }
    }

    static class ShutdownThread extends Thread {
        // 자바 애플리케이션의 경우 코드 내부에 셧다운 훅(shutdown hook)을 구현하여 안전한 종료를 명시적으로 구현할 수 있다.
        // 셧다운 훅이란 사용자 또는 운영체제로부터 종료 요청을 받으면 실행하는 스레드를 뜻한다.
        // 셧다운 훅이 발생하면 사용자가 정의한 ShutdownThread 스레드가 실행되면서 wakeup() 메서드가 호출되어 컨슈머를 안전하게 종료한다.
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}
