package com.scarycoders.learn.kafkaConsumer;

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

import java.io.IOException;

public class KafkaConsumer {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        String jsonString="{\"Foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
        IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.getId());
        client.close();
    }

    private static RestHighLevelClient createClient(){
        String userName="871ugc69obds";
        String password="3moi5kozzqds";
        String hostName="kafka-course-8026358643.ap-southeast-2.bonsaisearch.net";

        final CredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));

        RestClientBuilder restClientBuilder=RestClient.builder(new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
                    }
                });
        RestHighLevelClient restHighLevelClient=new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }
}
