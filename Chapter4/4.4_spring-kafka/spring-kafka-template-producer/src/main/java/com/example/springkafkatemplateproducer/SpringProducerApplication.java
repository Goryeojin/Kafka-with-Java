package com.example.springkafkatemplateproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.logging.Logger;

@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

	private static String TOPIC_NAME = "test";

	@Autowired
	private KafkaTemplate<String, String> customKafkaTemplate;
	// 빈 객체로 등록한 customKafkaTemplate 주입받도록 메서드 이름과 동일한 변수명 선언

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(SpringProducerApplication.class);
		application.run(args);
	}

	@Override
	public void run(String... args) {
		customKafkaTemplate.send(TOPIC_NAME, "test");
		ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "test");
		// send() 메서드를 사용해 특정 토픽으로 데이터를 전송한다.
		// 전송한 이후 정상 적재됐는지 여부를 확인하고 싶다면 ListenableFuture 메서드를 사용한다.
		future.addCallback(new KafkaSendCallback<String, String>() {
			// ListenableFuture 인스턴스에 addCallback 함수를 붙여 프로듀서가 보낸 데이터의 브로커 적재 여부를 비동기로 확인할 수 있다.
			// 만약 브로커에 정상 적재되었다면 onSuccess() 호출. 그렇지 않고 이슈 발생 시 onFailure() 호출.
			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println("???");
				System.out.println(result.toString());
			}
			@Override
			public void onFailure(KafkaProducerException ex) {
				System.out.println(ex.toString());
			}
		});
		System.exit(0);
	}

}
