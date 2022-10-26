package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    // 프로듀서는 생성한 레코드를 전송하기 위해 전송하고자 하는 토픽을 알고 있어야 한다.
    // 토픽을 지정하지 않고서는 데이터를 전송할 수 없다. 토픽 이름은 Producer Recored 인스턴스를 생성할 때 사용된다.
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    // 전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정한다.

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String messageValue = "testMessage"; // 메시지 값을 선언한다.

        /* Default Partitioner
        // Default Partitioner
        Properties configs = new Properties();
        // Properties에는 KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들을 key/value 값으로 선언한다.
        // 필수 옵션을 반드시 선언해야 하며, 선택 옵션은 선언하지 않으면 기본 옵션값으로 설정되어 작동한다.
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 전송할 카프카 클러스터 서버의 host와 IP 지정
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 메시지 키, 메시지 값을 직렬화하기 위한 직렬화 클래스를 선언한다.
        // String 객체를 전송하기 위해 String을 직렬화하는 클래스인 카프카 라이브러리의 StringSerializer를 사용한다.

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        // Properties를 KafkaProducer의 생성 파라미터로 추가하여 인스턴스를 생성한다. producer 인스턴스는 ProducerRecode 전송 시 사용된다.


//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성한다.
        // 여기서는 토픽 이름과 메시지 값만 선언하여 메시지 키는 null로 설정되어 전송된다.
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "Pangyo", "23");
        // 메시지 키가 포함된 레코드를 전송하려면 topic name, key, value 순으로 파라미터를 넣고 생성한다.
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, messageKey, messageValue);
        // 파티션을 직접 지정하고 싶다면 topic name, partition number, key, value 순으로 파라미터를 넣고 생성한다.

        producer.send(record);
        // 생성한 ProducerRecord를 전송하기 위해 record를 파라미터로 가지는 send() 메서드를 호출했다.
        // 프로듀서에서 send()는 즉각적인 전송이 아니라 파라미터로 들어간 record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다.
        // => '배치 전송' 이를 통해 카프카는 타 메시지 플랫폼과 차별화된 전송 속도를 갖는다.
        logger.info("{}", record);
        producer.flush();
        // flush()를 통해 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송한다.
        producer.close();
        // 애플리케이션을 종료하기 전에 close() 메서드를 호출하여 producer 인스턴스의 리소스들을 안전하게 종료한다.
        */

        // 커스텀 파티셔너를 가지는 프로듀서
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        // 커스텀 파티셔너를 지정한 경우 ProducerConfig의 PARTITIONER_CLASS_CONFIG 옵션을 사용자 생성 파티셔너로 설정하여
        // KafkaProducer 인스턴스를 생성해야 한다.
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /* 브로커 정상 전송 여부를 확인하는 프로듀서
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", messageValue);
        RecordMetadata metadata = producer.send(record).get();
        // KafkaProducer의 send() 메서드는 Future 객체를 반환한다.
        // 이 객체는 RecordMetadata의 비동기 결과를 표현하는 것으로 ProducerRecord가 카프카 브로커에 정상적으로 적재되었는지에 대한 데이터가 포함되어 있다.
        // get() 메서드를 사용하면 프로듀서로 보낸 데이터의 결과를 동기적으로 가져올 수 있다.
        // send()의 결괏값은 카프카 브로커로부터 응답을 기다렸다가 브로커로부터 응답이 오면 RecordMetadata 인스턴스를 반환한다.
        logger.info(metadata.toString());
        producer.flush();
        producer.close();
        */

        // 그러나 동기로 프로듀서의 전송 결과를 확인하는 것은 빠른 전송에 허들이 된다.
        // => 프로듀서가 전송하고 난 뒤 브로커로부터 전송에 대한 응답 값을 받기 전가지 대기하기 때문.
        // 프로듀서가 비동기로 결과를 확인할 수 있도록 Callback 인터페이스를 제공하고 있다.
        // 사용자 정의 Callback 클래스를 생성하여 레코드의 전송 결과에 대응하는 로직을 만들 수 있다.
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", messageValue);
        producer.send(record, new ProducerCallback());
        logger.info("{}", record);
        producer.flush();
        producer.close();
        // 레코드 전송 후 비동기로 결과를 받기 위해서는 KafkaProducer 인스턴스의 send() 메서드 호출 시
        // ProducerRecord 객체와 함께 사용자 정의 Callback 클래스를 넣으면 된다.
        // 비동기로 결과를 받을 경우 동기로 결과를 받는 경우보다 더 빠른 속도로 데이터를 추가 처리할 수 있지만
        // 전송하는 데이터의 순서가 중요한 경우 사용하면 안된다.
        // 비동기로 결과를 기다리는 동안 다음으로 보낼 데이터의 전송이 성공하고 앞서 보낸 데이터의 결과가 실패할 경우
        // 재전송으로 인해 데이터 순서가 역전될 수 있기 때문이다.
    }
}
