package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // 커넥트에서 SingleFileSinkConnector 커넥터 생성 시 받은 설정값 초기화
        this.configProperties = props;
        try {
            new SingleFileSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
            // 설정 초기화 시 필수 설정값이 없다면 ConnectException 발생시켜 커넥터 종료
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        // SingleFileSinkConnector 가 사용할 태스크 클래스 이름 지정
        return SingleFileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // 태스크가 2개 이상인 경우 태스크마다 다른 설정값 줄 때 사용.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i=0; i<maxTasks; i++)  {
            taskConfigs.add(taskProps);
        // 여기서는 동일한 설정값을 받도록 ArrayList 에 모두 동일한 설정 담음.
        }
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        // 커넥터에서 사용할 설정값 지정.
        return SingleFileSinkConnectorConfig.CONFIG;
    }

    @Override
    public void stop() {}
}
