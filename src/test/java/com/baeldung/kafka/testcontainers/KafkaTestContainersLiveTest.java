package com.baeldung.kafka.testcontainers;

import br.com.lucas.kafka.Application;
import br.com.lucas.kafka.usuario.Usuario;
import br.com.lucas.kafka.usuario.UsuarioData;
import com.baeldung.kafka.testcontainers.configs.KafkaTestContainersConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@AutoConfigureMockMvc
@SpringBootTest(classes = {
        KafkaTestContainersConfiguration.class,
        Application.class
})
public class KafkaTestContainersLiveTest {

    private final KafkaConsumer<String, SpecificRecord> consumer;
    private final KafkaTemplate<String, SpecificRecord> producer;
    private final SchemaRegistryClient schemaRegistryClient;
    private final AdminClient adminClient;
    private final Map<String, List<Schema>> subjectsByTopic;
    private final String topic;

    @Autowired
    public KafkaTestContainersLiveTest(
            @Value("${test.topic:}") String topic,
            KafkaConsumer<String, SpecificRecord> consumer, SchemaRegistryClient schemaRegistryClient, AdminClient adminClient, KafkaTemplate<String, SpecificRecord> producer) {
        this.consumer = consumer;
        this.schemaRegistryClient = schemaRegistryClient;
        this.adminClient = adminClient;
        this.producer = producer;
        this.topic = topic;
        this.subjectsByTopic = Map.of(topic, List.of(Usuario.getClassSchema()));
    }

    @BeforeAll
    void setup() throws ExecutionException, InterruptedException, RestClientException, IOException {
        createTopics();
        registerSchemas();
    }

    private void registerSchemas() throws RestClientException, IOException {
        for (var e : subjectsByTopic.entrySet()) {
            var topic = e.getKey();

            for (var schema : e.getValue()) {
                schemaRegistryClient.register(String.format("%s-%s", topic, schema.getFullName()), schema);
            }
        }
    }

    private void createTopics() throws ExecutionException, InterruptedException {
        final int partitions = 1;
        final short replicationFactor = 1;
        final var topics = subjectsByTopic.keySet();

        var existinsTopics = adminClient.listTopics().names().get();

        adminClient.createTopics(topics.stream()
                .filter(topic -> !existinsTopics.contains(topic))
                .map(topic -> new NewTopic(topic, partitions, replicationFactor))
                .collect(Collectors.toList())
        ).all().get();
    }

    @Test
    void deveEnviarMensagemParaOTopico() {
        var usuario = Usuario.newBuilder().setData(UsuarioData.newBuilder().setId("1").build()).build();
        producer.send(topic, usuario);
    }
}
