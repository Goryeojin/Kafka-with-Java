package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    ConsumerWorker(Properties prop, String topic, int number) {
        // KafkaConsumer 인스턴스를 생성하기 위해 필요한 변수를 컨슈머 스레드 생성자 변수로 받음.
        // 카프카 컨슈머 옵션을 담는 Properties 클래스, 토픽 이름, 스레드 번호를 받음.
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(prop);
        // KafkaConsumer 클래스는 스레드 세이프하지 않기 때문에 스레드별로 인스턴스를 별개로 만들어서 운영해야 함.
        // 만약 한 인스턴스를 여러 스레드에서 실행하면 ConcurrentModificationException 예외 발생.
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("thread:{}\trecord:{}", threadName, record);
                // poll() 메서드를 통해 리턴받은 레코드 처리. 어떤 스레드가 어떤 레코드 처리했는지 확인 가능.
            }
        }
    }
}
