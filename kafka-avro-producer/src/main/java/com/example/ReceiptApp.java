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

public class ReceiptApp {
    public static void main(String[] args) throws Exception {

        //Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "receipt-processor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("SOURCE_TOPIC"));


        // Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, ReceiptRecord> producer = new KafkaProducer<>(props);

        Signer signer = new Signer();

        System.out.println("Esperando mensajes en SOURCE_TOPIC...");
        // JSON de ejemplo
        // String jsonContent = "{\"event\":\"purchase\",\"amount\":100}";
//        String signature = signer.sign(jsonContent);
//
//        ReceiptRecord record = ReceiptRecord.newBuilder()
//                .setSignature(signature)
//                .setContent(jsonContent)
//                .build();
//
//
//        ProducerRecord<Object, ReceiptRecord> message =
//                new ProducerRecord<>("RECEIPT_01", null, record);
//
//
//        message.headers().add("producer_id", "receipt-generator".getBytes());
//
//        producer.send(message, (metadata, exception) -> {
//            if (exception == null) {
//                System.out.printf("Enviado a topic=%s partition=%d offset=%d%n",
//                        metadata.topic(), metadata.partition(), metadata.offset());
//            } else {
//                exception.printStackTrace();
//            }
//        });
        try {
            while (true) {
                //Consume el topic de Kafka sources
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    String jsonContent = record.value();
                    String signature = signer.sign(jsonContent);

                    // Construir el Avro record
                    ReceiptRecord receipt = ReceiptRecord.newBuilder()
                            .setSignature(signature)
                            .setContent(jsonContent)
                            .build();

                    // Crear ProducerRecord con key tipo String
                    ProducerRecord<String, ReceiptRecord> newRecord =
                            new ProducerRecord<>("RECEIPT_01", null, receipt);

                    // Copiar el header producer_id si existe
                    if (record.headers().lastHeader("producer_id") != null) {
                        newRecord.headers().add(
                                "producer_id",
                                record.headers().lastHeader("producer_id").value()
                        );
                    }

                    // Enviar mensaje a RECEIPT_01
                    producer.send(newRecord, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.printf("Enviado a topic=%s partition=%d offset=%d%n",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });
                }
            }
        } finally {
            // En caso de salir del bucle, cerrar productor y consumidor
            producer.flush();
            producer.close();
            consumer.close();
        }
    }
}

