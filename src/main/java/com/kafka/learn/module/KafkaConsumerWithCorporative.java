package com.kafka.learn.module;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerWithCorporative {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Inside Consumer with shut down");

        // Create the producer properties
        Properties properties = new Properties();
        String groupId = "consumer-group1";
        String topic = "secondtopic";

        // Connect to Local
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Connect to Cloud
        /*
         * properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
         * 
         * properties.setProperty("security.protocol", "SASL_SSL");
         * 
         * properties.setProperty("sasl.jaas.config",
         * "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4Eqcf7mq7fgQYYGCOdX6Y5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0RXFjZjdtcTdmZ1FZWUdDT2RYNlk1Iiwib3JnYW5pemF0aW9uSWQiOjc5LCJ1c2VySWQiOjY3MDcsImZvckV4cGlyYXRpb25DaGVjayI6IjBhYjIxMDhlLWRhZjItNDA1OC1hMmNhLWNkMzY2ZTAyNzJiMyJ9fQ.0mpnuZ1naAlCYSnMiqEk15piQYaeP2eq936Kk8aY0bU\";"
         * );
         * 
         * properties.setProperty("sasl.mechanism", "PLAIN");
         */

        // Set Consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        properties.setProperty("group.instance.id", "grpup1");
        // Create the Consumer

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main Thread
        final Thread mainThread = Thread.currentThread();

        // Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a Shutdown, Invoke the consumer.wakeup ");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Inside the error : {0}", e);
                }
            }

        });
        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll the data
            while (true) {

                log.info("polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> rec : records) {
                    log.info("key {0}  Value : {1} ", rec.key(), rec.value());

                    log.info("Partition " + rec.partition() + " Offset " + rec.offset());

                }

            }
        } catch (WakeupException we) {
            log.info("Wake up the Kafka Consumer ");

        } catch (Exception e) {
            log.error(" Unexpected Error");
        }finally {
            consumer.close();
            log.info("Consumer is closed now");
        }

    }
}
