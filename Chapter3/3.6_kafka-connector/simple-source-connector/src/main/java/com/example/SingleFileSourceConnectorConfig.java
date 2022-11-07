package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;

import java.util.Map;

public class SingleFileSourceConnectorConfig extends AbstractConfig {

    public static final String DIR_FILE_NAME = "file";
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "읽을 파일 경로와 이름";
    // 어떤 파일을 읽을 건지 지정해야하므로 옵션명 file 로 파일 위치와 이름을 값으로 받음.
    public static final String TOPIC_NAME = "topic";
    public static final String TOPIC_DEFAULT_VALUE = "test";
    public static final String TOPIC_DOC = "보낼 토픽 이름";
    // 읽은 파일을 어느 토픽으로 보낼 것인지 지정하기 위해 옵션명 topic 으로 1개의 토픽값을 받음.

    public static ConfigDef CONFIG = new ConfigDef().define(
            DIR_FILE_NAME, Type.STRING, DIR_FILE_NAME_DEFAULT_VALUE, Importance.HIGH, DIR_FILE_NAME_DOC)
            .define(TOPIC_NAME, Type.STRING, TOPIC_DEFAULT_VALUE, Importance.HIGH, TOPIC_DOC);
    // 읽을 파일 경로. 보낼 토픽 지정
    // 각 옵션값의 이름, 설명, 기본값, 중요도

    public SingleFileSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
