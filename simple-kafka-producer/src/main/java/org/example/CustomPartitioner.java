package org.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // partition 메서드에는 레코드를 기반으로 파티션을 정하는 로직이 포함된다.
        // 리턴값은 주어진 레코드가 들어갈 파티션 번호이다.

        if(keyBytes == null) { // 레코드에 메시지 키를 지정하지 않은 경우에는 비정상적인 데이터로 간주하고 InvalidRecordException을 발생시킨다.
            throw new InvalidRecordException("Need Message Key");
        }
        if(((String)key).equals("Pangyo")) { // 메시지 키가 Pangyo일 경우 파티션 0번으로 지정되도록 0을 리턴한다.
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        // 메시지 키가 Pangyo가 아닐 경우 해시값을 지정하여 특정 파티션에 매칭되도록 설정한다.
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
