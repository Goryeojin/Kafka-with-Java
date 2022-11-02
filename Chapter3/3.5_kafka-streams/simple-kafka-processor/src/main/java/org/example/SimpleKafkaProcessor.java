package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        // Topology 클래스는 프로세서 API를 사용한 토폴로지를 구성하기 위해 사용됨.
        topology.addSource("Source", STREAM_LOG)
                // stream_log 토픽을 소스 프로세서로 가져오기 위해 addSource() 사용. 소스 프로세서의 이름 입력, 대상 토픽 이름 입력
                .addProcessor("Process",
                        () -> new FilterProcessor(),
                        "Source")
                // 스트림 프로세서를 사용하기 위해 addProcessor() 사용. 스트림 프로세서 이름 입력. 사용자가 정의한 프로세서 인스턴스 입력. 부모 노드 입력(Source)
                .addSink("Sink",
                        STREAM_LOG_FILTER,
                        "Process");
                // stream_log_filter를 싱크 프로세서로 사용하여 데이터 저장하기 위해 addSink() 사용.
                // 싱크 프로세서 이름 입력, 저장할 토픽의 이름 입력, 부모 노드 입력(Process)

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
