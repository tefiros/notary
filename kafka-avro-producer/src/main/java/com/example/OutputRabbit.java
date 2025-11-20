package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
public class OutputRabbit {

    private final static String QUEUE_NAME = "q.logs.test";

    public static void main(String[] argv) throws Exception {

        // Datos de PostgreSQL
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String pass = "postgres";

        // Configurar RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
       //  factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        // SQL: solo los mensajes NO enviados
        String selectSql =
                "SELECT signature, content, ingest_ts, producer_id " +
                        "FROM \"RECEIPT_01\" WHERE sent = false";

        // SQL: actualizar como enviados
        String updateSql =
                "UPDATE \"RECEIPT_01\" SET sent = true WHERE signature = ?";

        try (com.rabbitmq.client.Connection rabbitConn = factory.newConnection();
             Channel channel = rabbitConn.createChannel()) {

            // Declarar la cola
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            //Conexión a PostgreSQL
            try (Connection pgConn = DriverManager.getConnection(url, user, pass);
                 PreparedStatement st = pgConn.prepareStatement(selectSql);
                 ResultSet rs = st.executeQuery()) {

                System.out.println("Leyendo tabla RECEIPT_01 y enviando mensajes a RabbitMQ…");

                // 3️Recorrer filas y enviar
                while (rs.next()) {

                    String signature = rs.getString("signature");
                    String content = rs.getString("content"); // ← ya es JSON
                    String ingestTs = rs.getString("ingest_ts");
                    String producerId = rs.getString("producer_id");

                    // Crear JSON final
                    String messageJson = String.format(
                            "{ \"signature\": \"%s\", \"content\": %s, \"ingest_ts\": \"%s\", \"producer_id\": \"%s\" }",
                            signature, content, ingestTs, producerId
                    );

                    // Enviar a RabbitMQ
                    channel.basicPublish("", QUEUE_NAME, null, messageJson.getBytes("UTF-8"));

                    System.out.println(" [x] Enviado → " + messageJson);

                    // Marcar como enviado
                    try (PreparedStatement up = pgConn.prepareStatement(updateSql)) {
                        up.setString(1, signature);
                        up.executeUpdate();
                    }
                }

            } catch (Exception e) {
                System.err.println("Error leyendo PostgreSQL:");
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Error conectando o enviando a RabbitMQ:");
            e.printStackTrace();
        }
//
//
//
//            // Lee el fichero como JSON
//            JsonNode node = mapper.readTree(new File("message.json"));
//            // Re-escribe como string compacto
//            String message = mapper.writeValueAsString(node);
//
//            //String message = "Hola desde Java y RabbitMQ!";
//            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
//
//            System.out.println(" [x] Enviado: '" + message + "'");
//        }
        // TEST SACAR FILAS DE TABLA POR PANTALLA
//        try (Connection conn = DriverManager.getConnection(url, user, pass);
//             PreparedStatement st = conn.prepareStatement(sql);
//             ResultSet rs = st.executeQuery()) {
//
//            System.out.println("Leyendo tabla RECEIPT_01…");
//
//            while (rs.next()) {
//
//                String signature = rs.getString("signature");
//                String content = rs.getString("content");
//                String ingestTs = rs.getString("ingest_ts");
//                String producerId = rs.getString("producer_id");
//
//                System.out.println("---- FILA ----");
//                System.out.println("signature: " + signature);
//                System.out.println("content: " + content);
//                System.out.println("ingest_ts: " + ingestTs);
//                System.out.println("producer_id: " + producerId);
//                System.out.println();
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
    }
}

