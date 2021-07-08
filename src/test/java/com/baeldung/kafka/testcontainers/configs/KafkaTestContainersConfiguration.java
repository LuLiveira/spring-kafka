package com.baeldung.kafka.testcontainers.configs;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@TestConfiguration
public class KafkaTestContainersConfiguration {

    @Bean
    public Network network(){
        return Network.newNetwork();
    }

    @Bean
    public KafkaContainer kafkaContainer(Network network){
        KafkaContainer container = new CustomKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3")).withNetwork(network);
        container.start();
        return container;
    }

    @Bean
    public ProducerFactory<String, SpecificRecord> producerFactory(KafkaContainer kafka, SchemaRegistryContainer schemaRegistryContainer) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        configProps.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, SpecificRecord> kafkaTemplate(ProducerFactory<String, SpecificRecord> producerFactory, SchemaRegistryContainer schemaRegistryContainer) {
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", schemaRegistryContainer.getUrl());
        return new KafkaTemplate<>(producerFactory, props);
    }

    @Bean
    public SchemaRegistryContainer schemaRegistryContainer(Network network, CustomKafkaContainer kafkaContainer){
        Map<String, String> props = new HashMap<>();
        props.put("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.getInternalUrl());
        SchemaRegistryContainer container = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:5.5.0"))
                .withEnv(props)
                .withNetwork(network)
                .waitingFor(Wait.forHttp("/subjects"));
        container.start();

        return container;
    }

    @Bean
    public SchemaRegistryClient schemaRegistryClient(SchemaRegistryContainer schemaRegistryContainer) {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaAvroSerializerConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
        props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);


        return new CachedSchemaRegistryClient(schemaRegistryContainer.getUrl(),
                1000,
                props);
    }

    @Bean
    public AdminClient kafkaAdminClient(KafkaContainer kafkaContainer){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

        return KafkaAdminClient.create(props);
    }

    @Bean
    public KafkaConsumer<String, SpecificRecord> consumerConfigs(KafkaContainer kafkaContainer, SchemaRegistryContainer schemaRegistryContainer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "baeldung");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
        props.put("schema.registry.url", schemaRegistryContainer.getUrl());
        return new KafkaConsumer<>(props);
    }

}
