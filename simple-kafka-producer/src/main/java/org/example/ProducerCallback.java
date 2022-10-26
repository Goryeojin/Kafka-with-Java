package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        // onCompletion 메서드는 레코드의 비동기 결과를 받기 위해 사용한다.
        // 브로커 적재에 이슈가 생겼을 경우 Exception에 어떤 에러가 발생하였는지 담겨서 메서드가 실행된다.
        // 에러가 발생하지 않았을 경우 RecordMetadata를 통해 해당 레코드가 적재된 토픽 이름과 파티션 번호, 오프셋을 알 수 있다.
        if(e != null) {
            logger.error(e.getMessage(), e);
        } else {
            logger.error(metadata.toString());
        }
    }
}
