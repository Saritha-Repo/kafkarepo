package com.kafka.learn.module;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWithCallBack.class.getSimpleName());

    public static void main(String args[]) {

        log.info("Kafka Producer with Call Back");

        // Create the producer properties
        Properties properties = new Properties();
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
        properties.setProperty("batch.size", "400");

       
        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j <= 5; j++) {

            for (int i = 0; i <= 10; i++) {

                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("secondtopic",
                        "Message Producing " + i);

                // Send the data

                producer.send(producerRecord, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // TODO Auto-generated method stub
                        if (e == null) {
                            log.info("topic " + metadata.topic() + "\n");

                            log.info("partition " + metadata.partition() + "\n");

                            log.info("offset " + metadata.offset() + "\n");

                            log.info("timestamp " + metadata.timestamp() + "\n");
                        }
                    }
                });
            }
             try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // Flush and close the producer

        producer.flush();

        producer.close();

    }

}
