package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    // 컨슈머 그룹 이름을 통해 컨슈머의 목적을 구분할 수 있다.
    // 컨슈머 그룹을 기준으로 컨슈머 오프셋을 관리하기 때문에
    // subscribe() 메서드를 사용하여 토픽을 구독하는 경우에는 컨슈머 그룹을 선언해야 한다.
    // 컨슈머가 중단되거나 재시작되더라도 컨슈머 그룹의 컨슈머 오프셋을 기준으로 이후 데이터를 처리하기 때문이다.
    // 컨슈머 그룹을 선언하지 않으면 어떤 그룹에도 속하지 않는 컨슈머로 동작하게 된다.


    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 프로듀서가 직렬화하여 전송한 데이터를 역직렬화하기 위해 역직렬화 클래스를 지정한다.
        // 메시지 키, 메시지 값에 대해 둘 다 역직렬화 클래스를 지정해야 하며,
        // 반드시 프로듀서에서 직렬화한 타입으로 역직렬화해야 한다.
        // 일치하지 않은 타입으로 역직렬화할 경우 정상적으로 타입 변환이 되지 않을 수도 있으므로 주의해야 한다.
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 자동 커밋할지 수동 커밋할지 선택한다. 기본값은 true

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        // Properties로 지정한 카프카 컨슈머 옵션을 파라미터로 받아 KafkaConsumer 인스턴스를 생성한다.
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        // 컨슈머에게 토픽을 할당하기 위해 subscribe() 메서드를 사용한다.
        // 이 메서드는 Collection 타입의 String 값들을 받는데, 1개 이상의 토픽 이름을 받을 수 있다.

        while(true) {
            // 컨슈머는 poll() 메서드를 호출하여 데이터를 가져와서 처리한다. 지속적으로 처리하기 위해 반복 호출한다.
            // while(true)처럼 무한루프를 만들어 내부에서 poll() 메서드를 통해 데이터를 가져오고 사용자가 원하는 데이터 처리를 수행한다.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            // 컨슈머는 poll() 메서드를 통해 ConsumerRecord 리스트를 반환한다.
            // poll() 메서드는 Duration 타입을 인자로 받는다.
            // 이 인자값은 브로커로부터 데이터를 가져올 때 컨슈머 버퍼에 데이터를 기다리기 위한 타임아웃 간격을 뜻한다.

            /* kafka-consumer-sync-commit
            for(ConsumerRecord<String, String> record : records) {
                // for loop를 통해 poll() 메서드가 반환한 ConsumerRecord 데이터들을 순차적으로 처리한다.
                logger.info("record:{}", record);
            }
            consumer.commitSync();
            // commitSync()는 poll() 메서드로 받은 가장 마지막 레코드의 오프셋을 기준으로 커밋한다.
            // 동기 오프셋 커밋을 사용할 경우에는 poll() 메서드로 받은 모든 레코드의 처리가 끝난 이후 commitSync() 메서드를 호출해야 한다.
            // 동기 커밋의 경우 브로커로 커밋을 요청한 후 커밋이 완료되기까지 기다린다. 자동 커밋이나 비동기 오프셋 커밋보다 동일 시간당 데이터 처리량이 적다는 특징이 있다.
            // commitSync()에 파라미터가 없으면 poll()로 반환된 가장 마지막 레코드의 오프셋을 기준으로 커밋된다.
            // 개별 레코드 단위로 매번 오프셋을 커밋하고 싶다면, commitSync() 메서드에 Map<TopicPartition, OffsetAndMetadata> 인스턴스를 파라미터로 넣으면 된다.
             */

            /* kafka-consumer-sync-offset-commit
            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
            // 현재 처리한 오프셋을 매번 커밋하기 위해 commitSynce() 메서드가 파라미터로 받을 HashMap 타입을 선언해야 한다.
            // HashMap의 키는 토픽과 파티션 정보가 담긴 Topic Partition 클래스가 되고 값은 오프셋 정보가 담긴 OffsetAndMetadata 클래스가 된다.
            for(ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                // 처리를 완료한 레코드의 정보를 토대로 Map<> 인스턴스에 키/값을 넣는다.
                // 현재 처리한 오프셋에 1을 더한 값을 커밋해야 한다. 컨슈머가 poll()을 수행할 때 마지막으로 커밋한 오프셋부터 레코드를 리턴하기 때문이다.
                consumer.commitSync(currentOffset);
            }
            */
            // kafka-consumer-async-commit
            for(ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
//            consumer.commitAsync();
            // 비동기 오프셋 커밋도 동기 커밋과 마찬가지로 poll() 메서드로 리턴된 가장 마지막 레코드를 기준으로 오프셋을 커밋한다.
            // 다만, 커밋이 완료될 때까지 응답을 기다리지 않고 비동기로 처리하여 동일 시간당 데이터 처리량이 더 많다.
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if(e != null)
                        System.out.println("Commit failed");
                    else
                        System.out.println("Commit succeeded");
                    if(e != null)
                        logger.error("Commit failed for offsets {}", offsets, e);
                }
            });
            // 비동기 오프셋 커밋을 사용할 경우 비동기로 커밋 응답을 받기 때문에 callback 함수를 파라미터로 받아서 결과를 얻을 수 있다.
            // OffsetCommitCallback 함수는 commitAsync()의 응답을 받을 수 있도록 도와주는 콜백 인터페이스이다.
            // 비동기로 받은 커밋 응답은 onComplete() 메서드를 통해 확인할 수 있다.
            // 정상 커밋시 Exception 변수는 null이고, 커밋 완료된 오프셋 정보가 Map<TopicPartition, OffsetAndMetadata>에 포함된다.
        }

    }
}
