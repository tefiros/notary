package com.example;

import example.avro.ReceiptRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;


public class ReceiptAppItrust {

    private static final Map<String, String> TOPIC_MAP;
    static {
        TOPIC_MAP = new HashMap<>();
        TOPIC_MAP.put("test-topic",     "test-signed-topic");
        TOPIC_MAP.put("aite-evidences", "aite-signed-evidences");
        TOPIC_MAP.put("rae-evidences",  "rae-signed-evidences");
    }

    public static void main(String[] args) throws Exception {

        // Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:29092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "receipt-processor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("test-topic", "aite-evidences", "rae-evidences"));

        // Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://schema-registry:8081");

        Producer<String, ReceiptRecord> producer = new KafkaProducer<>(props);

        Signer signer = new Signer();

        System.out.println("Esperando mensajes en: " + TOPIC_MAP.keySet());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    String outputTopic = TOPIC_MAP.get(record.topic());
                    if (outputTopic == null) {
                        System.out.println("Topic desconocido ignorado: " + record.topic());
                        continue;
                    }

                    String jsonContent = record.value();
                    String signature = signer.sign(jsonContent);

                    // Construir el Avro record
                    ReceiptRecord receipt = ReceiptRecord.newBuilder()
                            .setSignature(signature)
                            .setContent(jsonContent)
                            .build();

                    // Crear ProducerRecord con key tipo String
                    ProducerRecord<String, ReceiptRecord> newRecord =
                            new ProducerRecord<>(outputTopic, record.key(), receipt);

                    // Copiar el header producer_id si existe
                    if (record.headers().lastHeader("producer_id") != null) {
                        newRecord.headers().add(
                                "producer_id",
                                record.headers().lastHeader("producer_id").value()
                        );
                    }

                    producer.send(newRecord, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.printf("[%s -> %s] partition=%d offset=%d%n",
                                    record.topic(), metadata.topic(),
                                    metadata.partition(), metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });
                }
            }
        } finally {
            producer.flush();
            producer.close();
            consumer.close();
        }
    }
}