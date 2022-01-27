package com.cloudera;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducerSASL_SSL {

    public static void main(String[] args) {
        Properties properties = new Properties();
        //CHANGE ME
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<KAFKA-BROKER>:9093,<KAFKA-BROKER>:9093,<KAFKA-BROKER>:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "connectivity-testing");
        properties.put("sasl.kerberos.service.name", "kafka");

        //CHANGE ME
        properties.put("ssl.truststore.location", "/home/sunilemanjee/truststore.jks");
        
        //CHANGE
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<CDP-WORKLOAD-USER>\" password=\"<CDP-WORKLOAD-PASSWORD>\";");


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
