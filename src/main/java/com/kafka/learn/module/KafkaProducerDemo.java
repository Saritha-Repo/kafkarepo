package com.kafka.learn.module;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());

    public static void main(String args[]) {

        log.info("Simple Producer");

        // Create the producer properties
        Properties properties = new Properties();
        // Connect to Local
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Connect to Cloud

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        properties.setProperty("security.protocol", "SASL_SSL");

        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4Eqcf7mq7fgQYYGCOdX6Y5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0RXFjZjdtcTdmZ1FZWUdDT2RYNlk1Iiwib3JnYW5pemF0aW9uSWQiOjc5LCJ1c2VySWQiOjY3MDcsImZvckV4cGlyYXRpb25DaGVjayI6IjBhYjIxMDhlLWRhZjItNDA1OC1hMmNhLWNkMzY2ZTAyNzJiMyJ9fQ.0mpnuZ1naAlCYSnMiqEk15piQYaeP2eq936Kk8aY0bU\";");

        properties.setProperty("sasl.mechanism", "PLAIN");

        // properties.setProperty("", "");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("secondtopic", "Hello World");

        // Send the data
        
        producer.send(producerRecord);

        // Flush and close the producer
        
        producer.flush();
        
        producer.close();

    }

}
