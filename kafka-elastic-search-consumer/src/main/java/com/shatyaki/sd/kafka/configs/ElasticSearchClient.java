package com.shatyaki.sd.kafka.configs;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchClient {

    public static RestHighLevelClient createElasticSearchClient() {

        String hostname = ""; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String... agrs) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

//        Kafka consumer 
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
        Properties kafakaConsumerProperties = consumerConfig.getConsumerProperties("kafka-demo-app-1");

        String topic = "twitter-tweets";

        RestHighLevelClient client = createElasticSearchClient();

//        IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
//        String id = resp.getId();
//        logger.info("id : " + id);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafakaConsumerProperties);
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                // insert data into elastic search

//                Generic kafka id 
//                String messageId = record.topic() +"_"+ record.partition() +"_"+ record.offset();

//                Twitter feed specific id 
                String messageId = extractIdFromTweet(record.value());
                IndexRequest request = new IndexRequest("twitter").id(messageId).source(String.valueOf(record.value()),
                        XContentType.JSON);
                IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
                String id = resp.getId();
                logger.info("id : " + id);
                Thread.sleep(1000);
            }
        }
//        client.close();

    }

    private static String extractIdFromTweet(String tweet) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }

}
