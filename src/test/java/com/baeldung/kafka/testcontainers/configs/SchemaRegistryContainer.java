package com.baeldung.kafka.testcontainers.configs;

import org.springframework.lang.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private Map<String, String> props = new HashMap<>();

    public SchemaRegistryContainer(@NonNull DockerImageName dockerImageName){
        super(dockerImageName);

        props.put("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry");
        this.withEnv(props);
        this.withExposedPorts(8081);
        this.withNetworkAliases("schemaregistry");
    }

    public String getUrl(){
        return String.format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(8081));
    }
}
