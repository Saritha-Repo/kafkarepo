/**
 * 
 */
package com.kafka.learn.consumer.opensearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;

/**
 * @author saritha
 *
 */
public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        // String connString = "http://localhost:9200";

        String connString = "https://hlcx8p5ykd:i68hcztlks@kafka-cluster-8169231511.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(),
                                                                                          connUri.getPort(),
                                                                                          "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                                                          RestClient.builder(new HttpHost(connUri.getHost(),
                                                                                          connUri.getPort(),
                                                                                          connUri.getScheme()))
                                                                    .setHttpClientConfigCallback(
                                                                                                 httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(
                                                                                                                                                                                cp)
                                                                                                                                                 .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String boostrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                         .getAsJsonObject()
                         .get("meta")
                         .getAsJsonObject()
                         .get("id")
                         .getAsString();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        BulkRequest bulkRequest = new BulkRequest();

        try (openSearchClient; consumer) {
            if (!(openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT))) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("the Wikimedia index has benn created");
            } else {
                log.info(" the wikimedia index already exisitng");
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchanges"));

            while (true) {
                ConsumerRecords<String, String> conusmerRecords = consumer.poll(Duration.ofMillis(3000));
                int recordCount = conusmerRecords.count();
                log.info("Received Record Count :" + recordCount);

                for (ConsumerRecord<String, String> rec : conusmerRecords) {

                    // Strategy 1 - Consumer Delivery Semantics

                    // String id= rec.topic() + ""+ rec.partition()+""+ rec.offset();

                    // IndexRequest indexRequest = new IndexRequest("wikimedia").source(rec.value(),
                    // XContentType.JSON).id(id);

                    // Strategy 2 - Consumer Delivery Semantics - Extract id from the JSOn value

                    String id = extractId(rec.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(rec.value(), XContentType.JSON)
                                                                             .id(id);

                    bulkRequest.add(indexRequest);

                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    log.info(indexResponse.getId());
                }
            }
        } catch (IOException e) {

            e.printStackTrace();
        }

        try {
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {

            e.printStackTrace();
        }

        consumer.commitSync();

        // main code logic

    }

}
