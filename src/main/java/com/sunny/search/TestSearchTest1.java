package com.sunny.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.concurrent.TimeUnit;

public class TestSearchTest1 {
    public static void main(String[] args) {

    }

    public static void testMethod1(){
        // 1、创建search请求
        //SearchRequest searchRequest = new SearchRequest();
        SearchRequest searchRequest = new SearchRequest("bank");
        searchRequest.types("_doc");

        // 2、用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //构造QueryBuilder
            /*QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                    .fuzziness(Fuzziness.AUTO)
                    .prefixLength(3)
                    .maxExpansions(10);
            sourceBuilder.query(matchQueryBuilder);*/

        sourceBuilder.query(QueryBuilders.termQuery("age", 24));
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //是否返回_source字段
        //sourceBuilder.fetchSource(false);

        //设置返回哪些字段
            /*String[] includeFields = new String[] {"title", "user", "innerObject.*"};
            String[] excludeFields = new String[] {"_type"};
            sourceBuilder.fetchSource(includeFields, excludeFields);*/

        //指定排序
        //sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        //sourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC));

        // 设置返回 profile
        //sourceBuilder.profile(true);

        //将请求体加入到请求中
        searchRequest.source(sourceBuilder);

        // 可选的设置
        //searchRequest.routing("routing");

        // 高亮设置
            /*
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightTitle =
                    new HighlightBuilder.Field("title");
            highlightTitle.highlighterType("unified");
            highlightBuilder.field(highlightTitle);
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            highlightBuilder.field(highlightUser);
            sourceBuilder.highlighter(highlightBuilder);*/


        //加入聚合
            /*TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                    .field("company.keyword");
            aggregation.subAggregation(AggregationBuilders.avg("average_age")
                    .field("age"));
            sourceBuilder.aggregation(aggregation);*/

        //做查询建议
            /*SuggestionBuilder termSuggestionBuilder =
                    SuggestBuilders.termSuggestion("user").text("kmichy");
                SuggestBuilder suggestBuilder = new SuggestBuilder();
                suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
            sourceBuilder.suggest(suggestBuilder);*/
    }
}
