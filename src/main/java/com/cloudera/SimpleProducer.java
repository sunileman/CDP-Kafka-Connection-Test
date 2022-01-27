package com.cloudera;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<KAFKA-BROKER>:9092,<KAFKA-BROKER>:9092,<KAFKA-BROKER>:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "connectivity-testing");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        try {
            while (true) {
                Thread.sleep(1000);
                System.out.println("Sending message");
                kafkaProducer.send(
                        new ProducerRecord<String, String>("test", "test-key", "test message - from laptop"));
                System.out.println("message sent");

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }

    }

}
