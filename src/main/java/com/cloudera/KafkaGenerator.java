package com.cloudera;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
This class is used to stress test Kafka with large <threadCount> number of producers
 */
public class KafkaGenerator {

    public static void main(String... args) {
        int threadCount = 0;

        if (args.length < 5) {
            System.out.println("Please provide topic name, client name, nbr of threads, and thread wait time");
            System.exit(1);
        }

        final String topicName = args[0];
        //Each kafka producer thread will have clientName + threadName
        final String clientName = args[1];
        //threadCount is how many producer created for load testing
        threadCount = Integer.valueOf(args[2]);
        //waitTime is the amount of time each producer will wait before sending next messages
        int waitTime = Integer.valueOf(args[3]);
        //Kafka Brokers
        final String brokers = args[4];

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        Runnable producer = new Runnable() {

            public void run() {
                String threadName = Thread.currentThread().getName();
                Properties properties = new Properties();
                //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<KAFKA-BROKER>:9092,<KAFKA-BROKER>:9092,<KAFKA-BROKER>:9092");
                properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
                properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
                properties.put(ProducerConfig.ACKS_CONFIG, "all");
                properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientName + "-" + threadName);
                KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
                try {
                    while (true) {

                        kafkaProducer.send(new ProducerRecord<String, String>(topicName, threadName, "message from "+ clientName +" - " + threadName));
                        //System.out.println("Message Sent:"+threadName);
                        Thread.sleep(waitTime);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    kafkaProducer.close();
                }

            }
        };
        //long starttime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executor.execute(producer);
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
            if (executor instanceof ThreadPoolExecutor) {
                try {
                    Thread.sleep(5000);
                    ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
                    System.out.println("Active Threads - " + pool.getActiveCount());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

        //System.out.println("Total time: "+ (System.currentTimeMillis() - starttime)/1000);

    }

}
