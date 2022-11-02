package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinTable {

    private static String APPLICATION_NAME = "order-join-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address";
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        orderStream.join(addressTable,
                (order, address) -> order + " send to " + address)
                .to(ORDER_JOIN_STREAM);
        // KStrea, KTable에서 동일한 메시지 키를 가진 데이터를 찾았을 경우 각각의 메시지 값을 조합해서 어떤 데이터를 만들기 정한다.
        // order 토픽의 물품 이름과 address 토픽의 주소를 조합하여 새로운 메시지 값을 만든다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
