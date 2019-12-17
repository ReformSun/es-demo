package com.sunny.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ClientUtils {
    private static final String es_host = "10.4.247.16";
    private static final int es_port = 9200;
    public static RestHighLevelClient initESClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(es_host, es_port, "http")));
        return client;
    }
}
