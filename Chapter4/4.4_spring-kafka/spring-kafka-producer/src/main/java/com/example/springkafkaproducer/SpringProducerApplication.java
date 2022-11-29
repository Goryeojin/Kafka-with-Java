package com.example.springkafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import javax.swing.*;

@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    @Autowired
    private KafkaTemplate<Integer, String> template;
    // KafkaTemplate 을 @Autowired 어노테이션으로 주입받아서 사용함.
    // 스프링 카프카에서 제공하는 기본 KafkaTemplate 객체로 주입됨. application.yaml 에 설정값 자동 주입.

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run (String... args) {
        for (int i=0; i<10; i++) {
            template.send(TOPIC_NAME, "test" + i);
            // send() -> 토픽 이름, 메시지 값 넣어 전송
        }
        System.exit(0);
        // 전송 완료 시 정상 종료
    }
}
