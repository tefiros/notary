package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import example.avro.FooRecord;

import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, FooRecord> producer = new KafkaProducer<>(props);

        FooRecord record = FooRecord.newBuilder()
                .setCOL1(0)
                .setCOL2(0)
                .build();

        ProducerRecord<String, FooRecord> message =
                new ProducerRecord<>("FOO_01", null, record);

        
        message.headers().add("producer_id", "app-123".getBytes());

        producer.send(message, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Enviado a topic=%s partition=%d offset=%d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}

