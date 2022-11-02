package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application";
    // 기존에 사용하였던 application.id 와 중복되지 않게 지정한다.
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        // stream_log 토픽을 가져오기 위한 소스 프로세서 작성. stream()으로 토픽의 데이터를 KStream 인스턴스로 만들었다.
//        KStream<String, String> filteredStream = streamLog.filter(
//                (key, value) -> value.length() > 5);
//        // 데이터를 필터링하는 filter() : 자바의 함수형 인터페이스인 Predicate를 파라미터로 받음.
//        // Predicate => 특정 조건을 표현할 때 사용. 메시지 키와 값의 조건으로 value.length() > 5 로 필터링
//        filteredStream.to(STREAM_LOG_FILTER);
        // 필터링된 KStream을 stream_log_filter 토픽에 저장하도록 소스 프로세서 작성

        streamLog.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
