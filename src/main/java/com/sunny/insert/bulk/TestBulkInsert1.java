package com.sunny.insert.bulk;

import com.sunny.insert.SendRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class TestBulkInsert1 {
    public static void main(String[] args) {
        testMethod1();
    }

    public static void testMethod1(){
        // 1、创建批量操作请求
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("mess", "_doc", "1")
                .source(XContentType.JSON,"field", "foo"));
        request.add(new IndexRequest("mess", "_doc", "2")
                .source(XContentType.JSON,"field", "bar"));
        request.add(new IndexRequest("mess", "_doc", "3")
                .source(XContentType.JSON,"field", "baz"));
        SendRequest.sendBulkRequest(request);
    }
}
