/**
 * 
 */
package com.kafka.wikimedia.module;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;



/**
 * @author saritha
 *
 */
public class WikimediaChangesProducer {

    /**
     * @param args
     */
    public static void main(String[] args) {

        String bootStrapServers = "localhost:9092";

        String topic = "wikimedia-changes";

        // Create Producer properties

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        
        //Set high throughput producer config
        properties.setProperty("linger.ms", Integer.toString(20));
        properties.setProperty("batch.size", Integer.toString(32*1024));
        properties.setProperty("compression.type", "snappy");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource = builder.build();

        // Start the producer in another thread

        eventSource.start();
        
        
        //Produce for 10 minutes and block then 
        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            
            e.printStackTrace();
        }
    }

}
