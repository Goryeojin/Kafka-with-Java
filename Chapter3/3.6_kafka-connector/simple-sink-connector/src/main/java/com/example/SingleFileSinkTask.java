package com.example;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {

    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter fileWriter;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // 커넥터 실행 시 설정한 옵션을 토대로 리소스 초기화.
        try {
            config = new SingleFileSinkConnectorConfig(props);
            file = new File(config.getString(config.DIR_FILE_NAME));
            fileWriter = new FileWriter(file, true);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // 일정 주기로 토픽의 데이터를 가져오는 put(). 데이터 저장 코드 작성
        try {
            for (SinkRecord record : records) {
                // SinkRecord 는 토픽의 레코드로 토픽, 파티션, 타임스탬프 정보를 포함함.
                fileWriter.write(record.value().toString() + "\n");
                // 레코드의 메시지 값을 가져오기 위해 value() 리턴받은 객체를 String 으로 포맷해 저장.
            }
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // put() 에서 파일에 데이터를 저장하는 것은 버퍼에 데이터를 저장하는 것으로
        // 실질적으로 파일 시스템에 데이터를 저장하려면 FileWriter 클래스의 flush() 호출해야함.
        try {
            fileWriter.flush();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        // 태스크 종료 시 열고 있던 파일을 안전하게 닫는다.
        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }
}
