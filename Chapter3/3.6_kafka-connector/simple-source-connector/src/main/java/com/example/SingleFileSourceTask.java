package com.example;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleFileSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(SingleFileSourceTask.class);

    public final String FILENAME_FIELD = "filename";
    public final String POSITION_FIELD = "position";
    // 파일 이름과 해당 파일을 읽은 지점을 오프셋 스토리지에 저장하기 위해 filename, position 값을 정의함.
    // 이 2개의 키를 기준으로 오프셋 스토리지에 읽은 위치 저장.

    private Map<String, String> fileNamePartition;
    private Map<String, Object> offset;
    private String topic;
    private String file;
    // 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map 자료구조에 담은 데이터 사용.
    // filename이 키, 커넥터가 읽은 파일 이름이 값으로 저장되어 사용됨.

    private long position = -1;
    // 읽은 파일의 위치를 커넥터 멤버 변수로 지정하여 사용.
    // 커넥터 최초 실행 시 오프셋 스토리지에서 마지막으로 읽은 파일의 위치를 position 변수에 선언해 중복 적재하지 않도록 한다.
    // 처음 읽은 파일이라면 오프셋 스토리지에 파일을 읽은 기록이 없으므로 position 을 0으로 설정한다.

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            // Init variables
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            // 커넥터 실행 시 받은 설정값을 SingleFileSourceConnectorConfig 로 선언하여 사용.
            // 토픽 이름과 파일 이름은 SingleFileSourceTask 의 멤버변수로 선언되어 있기 때문에 start() 에서 초기화 후 다른 메서드에서 사용 가능.
            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);
            offset = context.offsetStorageReader().offset(fileNamePartition);

            // Get file offset from offsetStorageReader
            if(offset != null) {
                // 오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져온다.
                // 오프셋 스토리지는 실제 데이터가 저장되는 곳으로. 단일 모드 커넥터는 로컬 파일로 저장, 분산 모드 커넥트는 내부 토픽에 저장한다.
                // offset 이 null 이면 읽고자하는 데이터가 없다는 뜻이다. null 이 아니면 한 번이라도 커넥터를 통해 해당 파일을 처리했다는 것이다.
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                // 해당 파일에 대한 정보가 있을 경우 파일의 마지막 읽은 위치를 get() 메서드를 통해 가져온다.
                if(lastReadFileOffset != null) {
                    position = (Long) lastReadFileOffset; // 오프셋 스토리지에서 가져온 마지막 처리 지점을 position 에 할당.
                    // 커넥터가 재시작되더라도 데이터의 중복, 유실 처리를 막는다.
                }
            } else {
                position = 0;
                // 반면 오프셋 스토리지에서 가져온 데이터가 null 이라면 파일 처리한 적이 없다는 것으로 0을 할당한다.
                // position 을 0으로 설정하면 파일의 첫째 줄부터 처리해 토픽으로 데이터를 보냄.
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> result = new ArrayList<>();
        // poll() => 태스크 시작 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출됨.
        // 소스 파일의 데이터를 읽어 토픽으로 데이터를 보내는 로직 작성.
        // 토픽으로 데이터 보내는 방법 => return List<SourceRecord>
        // SourceRecord: 토픽으로 보낼 데이터 담는 클래스.
        try {
            Thread.sleep(1000);

            List<String> lines = getLines(position);
            // 토픽으로 보내기 전 파일에서 한 줄씩 읽어와야 한다. 마지막으로 읽었던 지점 이후로 파일의 마지막까지 읽어 리턴하는 getLines() 로 데이터를 받는다.
            if (lines.size() > 0) {
               lines.forEach(line -> {
                // 리턴받은 데이터는 각 줄을 한 개의 SourceRecord 인스턴스로 생성해 results 변수에 넣는다.
                   Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                   SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                   // SourceRecord 인스턴스를 만들 때는 마지막으로 전송한 데이터의 위치를 오프셋 스토리지에 저장하기 위해
                   // 앞서 선언한 fileNameParition, 현재 토픽으로 보내는 줄의 위치를 기록한 sourceOffset 을 변수로 넣어 선언한다.
                   result.add(sourceRecord);
               });
            }
           return result;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);
        }
    }

    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
        // BufferedReader 로 파일을 읽어 각 줄을 String 으로 변환하고 List 형으로 리턴한다.
    }

    @Override
    public void stop() {

    }
}
