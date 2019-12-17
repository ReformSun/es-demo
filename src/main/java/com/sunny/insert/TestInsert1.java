package com.sunny.insert;

import com.sunny.client.ClientUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.sunny.insert.SendRequest.sendRequest;

public class TestInsert1 {
    public static void main(String[] args) {
        testMethod1();
    }
    // 方式一：直接给JSON串
    public static void testMethod1(){
        // 1、创建索引请求
        IndexRequest request = new IndexRequest(
                "mess",   //索引
                "_doc",     // mapping type
                "1");     //文档id
        // 2、准备文档数据
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        sendRequest(request);
    }

    // 方式二：以map对象来表示文档
    public static void testMethod2(){
        // 1、创建索引请求
        IndexRequest request = new IndexRequest(
                "mess",   //索引
                "_doc",     // mapping type
                "1");     //文档id
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        request.source(jsonMap);
        sendRequest(request);
    }

    // 方式三：用XContentBuilder来构建文档
    public static void testMethod3(){
        // 1、创建索引请求
        IndexRequest request = new IndexRequest(
                "mess",   //索引
                "_doc",     // mapping type
                "1");     //文档id

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.field("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            request.source(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        sendRequest(request);
    }

    // 方式四：直接用key-value对给出
    public static void testMethod4(){
        // 1、创建索引请求
        IndexRequest request = new IndexRequest(
                "mess",   //索引
                "_doc",     // mapping type
                "1");     //文档id
        request.source("user", "kimchy",
                "postDate", new Date(),
                "message", "trying out Elasticsearch");
        sendRequest(request);
    }




}
