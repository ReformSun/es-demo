package com.sunny.insert;

import com.sunny.client.ClientUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class SendRequest {
    /**
     * 同步请求
     * @param indexRequest
     */
    public static void sendRequest(IndexRequest indexRequest){

        indexRequest.routing("routing");  //设置routing值
        indexRequest.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
        indexRequest.setRefreshPolicy("wait_for");  //设置重刷新策略
        indexRequest.version(2);  //设置版本号
        indexRequest.opType(DocWriteRequest.OpType.CREATE);  //操作类别

        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            //4、发送请求
            IndexResponse indexResponse = null;
            try {
                // 同步方式
                indexResponse = client.index(indexRequest);
            } catch(ElasticsearchException e) {
                // 捕获，并处理异常
                //判断是否版本冲突、create但文档已存在冲突
                if (e.status() == RestStatus.CONFLICT) {
                    System.out.println("冲突了，请在此写冲突处理逻辑！\n" + e.getDetailedMessage());
                }
                System.out.println("索引异常" + e.getDetailedMessage());
            }

            //5、处理响应
            if(indexResponse != null) {
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                String id = indexResponse.getId();
                long version = indexResponse.getVersion();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功，处理逻辑代码写到这里。");
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功，处理逻辑代码写到这里。");
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

                }
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步请求
     * @param indexRequest
     */
    public static void sendAsyncRequest(IndexRequest indexRequest){
        indexRequest.routing("routing");  //设置routing值
        indexRequest.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
        indexRequest.setRefreshPolicy("wait_for");  //设置重刷新策略
        indexRequest.version(2);  //设置版本号
        indexRequest.opType(DocWriteRequest.OpType.CREATE);  //操作类别

        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    //5、处理响应
                    if(indexResponse != null) {
                        String index = indexResponse.getIndex();
                        String type = indexResponse.getType();
                        String id = indexResponse.getId();
                        long version = indexResponse.getVersion();
                        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                            System.out.println("新增文档成功，处理逻辑代码写到这里。");
                        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                            System.out.println("修改文档成功，处理逻辑代码写到这里。");
                        }
                        // 分片处理信息
                        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

                        }
                        // 如果有分片副本失败，可以获得失败原因信息
                        if (shardInfo.getFailed() > 0) {
                            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                                String reason = failure.reason();
                                System.out.println("副本失败原因：" + reason);
                            }
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {

                }
            };
            client.indexAsync(indexRequest, listener);


        }catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 批量
     * 同步请求
     * @param indexRequest
     */
    public static void sendBulkRequest(BulkRequest request){
//       可选的一些配置
        request.timeout("2m");
        request.setRefreshPolicy("wait_for");
        request.waitForActiveShards(2);
        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            BulkResponse bulkResponse = client.bulk(request);
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
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
