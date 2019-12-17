package com.sunny.bulk.processor.insert;

import com.sunny.bulk.processor.BulkProcessorUtil;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.UnknownHostException;

public class TestInsert1 {
    public static void main(String[] args) throws UnknownHostException {
        testMethod1();
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void testMethod1() throws UnknownHostException {
        BulkProcessor bulkProcessor = BulkProcessorUtil.getBulkProcessor();
        bulkProcessor.add(new IndexRequest("mess", "_doc", "4")
                .source(XContentType.JSON,"field", "foo"));
        bulkProcessor.add(new IndexRequest("mess", "_doc", "5")
                .source(XContentType.JSON,"field", "foo"));
        bulkProcessor.add(new IndexRequest("mess", "_doc", "6")
                .source(XContentType.JSON,"field", "foo"));
        bulkProcessor.add(new IndexRequest("mess", "_doc", "7")
                .source(XContentType.JSON,"field", "foo"));
        bulkProcessor.add(new IndexRequest("mess", "_doc", "8")
                .source(XContentType.JSON,"field", "foo"));
    }
}
