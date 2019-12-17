package com.sunny.bulk.processor;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 批触发的策略
 * 1. 按照处理请求积压数触发server的请求（主动）
 * 2. 按照处理请求积压的字节数触发server请求（主动）
 * 3. 定时触发server请求（定时）
 */
public class BulkProcessorUtil {
    public static final Logger logger = LoggerFactory.getLogger(BulkProcessorUtil.class);
    public static BulkProcessor getBulkProcessor() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.4.247.16"), Integer.parseInt("9300")));

        return BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                System.out.println(bulkResponse.toString());
                //处理响应
                if(bulkResponse != null) {
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                        if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                                || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                            IndexResponse indexResponse = (IndexResponse) itemResponse;
                            //TODO 新增成功的处理

                        } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                            UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                            //TODO 修改成功的处理

                        } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                            DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                            //TODO 删除成功的处理
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                System.out.println(throwable.toString());
                logger.error("{} data bulk failed,reason :{}", bulkRequest.numberOfActions(), throwable);
            }

        }).setBulkActions(1000)
                .setBulkActions(10000) // 1w次请求执行一次bulk
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) // 5MB的数据刷新一次bulk
                .setFlushInterval(TimeValue.timeValueSeconds(5)) // 固定5s必须刷新一次
                .setConcurrentRequests(1) // 设置并发请求数当值为0时只允许一个请求执行
                // 设置退避, 100ms后执行, 最大请求3次  如果连接失败重试的初始时间间隔为100ms，如果重试失败的话，下一次重试时间间隔也会随之增长
                // 第一次重试失败   100ms
                // 第二次重试失败    500ms
                // 第三次重试失败    2s
                // 超时这东西如果在一段时间内发生的话，你重试多少次都没有用，所以他为了不浪费性能，如果多次尝试都不成功那就延长重试的时间间隔。
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();




    }
}
