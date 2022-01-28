package com.cloudera;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SimpleConsumerSASL_SSL {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "<KAFKA-BROKER>:9093,<KAFKA-BROKER>:9093,<KAFKA-BROKER>:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.kerberos.service.name", "kafka");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");

        properties.put("ssl.truststore.location", "/home/sunilemanjee/truststore.jks");
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<CDP-WORKLOAD-USER>\" password=\"<CDP-WORKLOAD-PASSWORD>\";");



        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        List<String> topics = new ArrayList<String>();
        topics.add("test");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                int count = 0;
                ConsumerRecords<String, String> records = kafkaConsumer.poll(5000);
                for (ConsumerRecord<String, String> record: records){
                    count++;
                }
                System.out.println("Consumed Messages"+ count);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}