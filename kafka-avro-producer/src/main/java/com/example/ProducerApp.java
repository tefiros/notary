package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProducerApp {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        ObjectMapper mapper = new ObjectMapper();
        // Lee el fichero como JSON
        JsonNode node = mapper.readTree(new File("message.json"));
        // Re-escribe como string compacto
        String payload = mapper.writeValueAsString(node);


        ProducerRecord<String, String> record =
                new ProducerRecord<>("SOURCE_TOPIC", null, payload);

        // Header original
        record.headers().add("producer_id", "WhiteShark".getBytes());

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("SOURCE_TOPIC -> partition=%d offset=%d%n",
                        metadata.partition(), metadata.offset());
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}
