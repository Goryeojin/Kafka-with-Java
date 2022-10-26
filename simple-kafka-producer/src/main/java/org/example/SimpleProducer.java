package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    // 프로듀서는 생성한 레코드를 전송하기 위해 전송하고자 하는 토픽을 알고 있어야 한다.
    // 토픽을 지정하지 않고서는 데이터를 전송할 수 없다. 토픽 이름은 Producer Recored 인스턴스를 생성할 때 사용된다.
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    // 전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정한다.

    public static void main(String[] args) {

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

        String messageValue = "testMessage"; // 메시지 값을 선언한다.
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성한다.
        // 여기서는 토픽 이름과 메시지 값만 선언하여 메시지 키는 null로 설정되어 전송된다.
        producer.send(record);
        // 생성한 ProducerRecord를 전송하기 위해 record를 파라미터로 가지는 send() 메서드를 호출했다.
        // 프로듀서에서 send()는 즉각적인 전송이 아니라 파라미터로 들어간 record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다.
        // => '배치 전송' 이를 통해 카프카는 타 메시지 플랫폼과 차별화된 전송 속도를 갖는다.
        logger.info("{}", record);
        producer.flush();
        // flush()를 통해 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송한다.
        producer.close();
        // 애플리케이션을 종료하기 전에 close() 메서드를 호출하여 producer 인스턴스의 리소스들을 안전하게 종료한다.
    }
}
