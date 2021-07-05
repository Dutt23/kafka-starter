package com.shatyaki.sd.kafka.configs;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchClient {

    
    public static RestHighLevelClient createElasticSearchClient() {
        
        String hostname = ""; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    
    
    public static void main(String ...agrs) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
        RestHighLevelClient client = createElasticSearchClient();
        String jsonObject = "{\"age\":10,\"dateOfBirth\":1471466076564,"
                +"\"fullName\":\"John Doe\"}";
        IndexRequest request = new IndexRequest("twitter").source(jsonObject, XContentType.JSON);
        
        IndexResponse resp = client.index(request, RequestOptions.DEFAULT);
        String id = resp.getId();
        logger.info("id : " + id);
        
        client.close();
        
    }
    
}


