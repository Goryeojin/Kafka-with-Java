package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        // application.id 지정. 애플리케이션 아이디 값을 기준으로 병렬처리함.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보 입력.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // 스트림 처리를 위해 메시지 키와 값의 역직렬화, 직렬화 방식 지정.
        // 애플리케이션에서는 데이터 처리 시 메시지 키 또는 값을 역직렬화하여 사용. 최종적으로 토픽에 데이터를 넣을 때 직렬화하여 데이터 저장.

        StreamsBuilder builder = new StreamsBuilder();
        // StreamsBuilder => 스트림 토폴로지 정의를 위해 사용
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        // stream_log 토픽으로부터 KStream 객체를 만들기 위해 StreamsBuilder의 stream() 사용.
        // stream(), table(), globalTable() => 최초의 토픽 데이터를 가져오는 소스 프로세서.
        streamLog.to(STREAM_LOG_COPY);
        // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기 위해 to() 사용.
        // to() => KStream 인스턴스의 데이터들을 특정 토픽으로 저장하기 위한 용도. => 싱크 프로세서

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        // StreamsBuilder로 정의한 토폴로지에 대한 정보, 스트림즈 실행을 위한 기본 옵션을 파라미터로 갖는 KafkaStreams 인스턴스 생성.
        // 실행을 위해 start() 사용. stream_log 토픽의 데이터 --> stream_log_copy 토픽으로 전달
    }
}
