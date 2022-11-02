package org.example;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcessor implements Processor<String, String> {
    // 스트림 프로세서 클래스 생성 Processor 인터페이스 구현.
    private ProcessorContext context;
    // ProcessorContext 클래스는 프로세서에 대한 정보를 담고 있음.
    // 현재 스트림 처리 중인 토폴로지의 토픽 정보, 애플리케이션 아이디 조회
    // schedule(), forward(), commit() 등 프로세싱 처리

    @Override
    public void init(ProcessorContext context) {
        // init() => 스트림 프로세서의 생성자. 프로세싱 처리에 필요한 리소스를 선언할 수 있다.
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        // 실질적인 프로세싱 로직.
        if (value.length() > 5) {
            context.forward(key, value);
            // forward() 를 사용하여 다음 토폴로지(다음 프로세서)로 넘어가도록 함.
        }
        context.commit();
        // 처리가 완료되면 commit()을 호출하여 명시적으로 데이터가 처리되었음을 선언.
    }

    @Override
    public void close() {
        // FilterProcessor가 종료되기 전 호출되는 메서드.
        // 프로세싱을 위해 사용한 리소스를 해제하는 구문을 작성한다.
    }
}
