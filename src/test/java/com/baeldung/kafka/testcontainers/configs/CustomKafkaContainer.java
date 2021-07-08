package com.baeldung.kafka.testcontainers.configs;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class CustomKafkaContainer extends KafkaContainer {

    public CustomKafkaContainer(DockerImageName dockerImageName){
        super(dockerImageName);

        this.withNetworkAliases("kafkabroker");
    }

    public String getInternalUrl(){
        return String.format("%s:%d", "kafkabroker", 9092);
    }

}
