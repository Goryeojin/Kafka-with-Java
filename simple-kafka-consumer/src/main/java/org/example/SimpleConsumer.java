package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
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
            for(ConsumerRecord<String, String> record : records) {
                // for loop를 통해 poll() 메서드가 반환한 ConsumerRecord 데이터들을 순차적으로 처리한다.
                logger.info("{}", record);
            }
        }
    }
}
