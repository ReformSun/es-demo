package com.sunny.search;

import com.sunny.client.ClientUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Map;

public class SendRequest {
    private static Logger logger = LogManager.getRootLogger();
    /**
     * 同步请求
     * @param indexRequest
     */
    public static void sendRequest(SearchRequest request){
        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            //3、发送请求
            SearchResponse searchResponse = client.search(request);


            //4、处理响应
            //搜索结果状态信息
            RestStatus status = searchResponse.status();
            TimeValue took = searchResponse.getTook();
            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            boolean timedOut = searchResponse.isTimedOut();

            //分片搜索情况
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            int failedShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                // failures should be handled here
            }

            //处理搜索命中文档结果
            SearchHits hits = searchResponse.getHits();

            long totalHits = hits.getTotalHits();
            float maxScore = hits.getMaxScore();

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit

                String index = hit.getIndex();
                String type = hit.getType();
                String id = hit.getId();
                float score = hit.getScore();

                //取_source字段值
                String sourceAsString = hit.getSourceAsString(); //取成json串
                Map<String, Object> sourceAsMap = hit.getSourceAsMap(); // 取成map对象
                //从map中取字段值
                /*
                String documentTitle = (String) sourceAsMap.get("title");
                List<Object> users = (List<Object>) sourceAsMap.get("user");
                Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
                */
                logger.info("index:" + index + "  type:" + type + "  id:" + id);
                logger.info(sourceAsString);

                //取高亮结果
                /*Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("title");
                Text[] fragments = highlight.fragments();
                String fragmentString = fragments[0].string();*/
            }

            // 获取聚合结果
            /*
            Aggregations aggregations = searchResponse.getAggregations();
            Terms byCompanyAggregation = aggregations.get("by_company");
            Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
            Avg averageAge = elasticBucket.getAggregations().get("average_age");
            double avg = averageAge.getValue();
            */

            // 获取建议结果
            /*Suggest suggest = searchResponse.getSuggest();
            TermSuggestion termSuggestion = suggest.getSuggestion("suggest_user");
            for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
                for (TermSuggestion.Entry.Option option : entry) {
                    String suggestText = option.getText().string();
                }
            }
            */
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void sendAsyncRequest(SearchRequest request){


        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse getResponse) {
                    //结果获取
                }

                @Override
                public void onFailure(Exception e) {
                    //失败处理
                }
            };
            client.searchAsync(request, listener);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
