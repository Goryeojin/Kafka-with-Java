package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;

import java.util.Map;

public class SingleFileSinkConnectorConfig extends AbstractConfig {
    // 커넥터 실행 시 받을 설정값들 정의
    public static final String DIR_FILE_NAME = "file";
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "저장할 디렉토리와 파일";
    // 토픽의 데이터를 저장할 파일 이름을 옵션값으로 받기 위해 선언.
    // 토픽이 옵션값에 없는 이유는 커넥트를 통해 커넥터 실행 시 기본값으로 받아야 하기 때문에 싱크 커넥터의 옵션으로 토픽은 추가하지 않아도 됨.

    public static ConfigDef CONFIG = new ConfigDef()
            .define(DIR_FILE_NAME, Type.STRING, DIR_FILE_NAME_DEFAULT_VALUE, Importance.HIGH, DIR_FILE_NAME_DOC);
    // ConfigDef 는 커넥터에서 사용할 옵션값들에 대한 정의를 표현한다.
    // 각 옵션값의 이름, 설명, 기본값, 중요도 지정

    public SingleFileSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
