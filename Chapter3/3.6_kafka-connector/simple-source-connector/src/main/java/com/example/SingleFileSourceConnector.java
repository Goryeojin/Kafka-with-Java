package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSourceConnector extends SourceConnector {
    // SourceConnector 추상클래스를 상속받음. SingleFileSourceConnector 는 커넥터에서 사용할 커넥터 이름이 된다.
    // 플러그인으로 추가하여 사용 시에는 패키지 이름과 함께 붙여서 사용된다.
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            new SingleFileSourceConnectorConfig(props);
            // 커넥트에서 SingleFileSourceConnector 커넥터를 생성할 때 받은 설정값들 초기화
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
            // 설정을 초기화할 때 필수 설정값이 빠져있다면 ConnectException 발생시키고 커넥터 종료.
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
        // SingleFileSourceConnector 가 사용할 태스크의 클래스 이름 지정
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용하는 메서드.
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for(int i=0; i<maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        // 2개 이상이더라도 같은 설정값을 받도록 arrayList에 동일한 설정을 담았다.
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        // 커넥터에서 사용할 설정값 지정.
        // 여기서는 SingleFileSourceConnectorConfig 의 멤버 변수로 정의된 CONFIG 인스턴스 리턴
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    @Override
    public void stop() {
        // 커넥터 종료 시 필요한 로직 추가
    }
}
