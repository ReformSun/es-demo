package com.sunny.get;

import com.sunny.client.ClientUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

public class TestGet1 {
    public static void main(String[] args) {

    }

    public static void testMethod1(){
        try (RestHighLevelClient client = ClientUtils.initESClient();) {
            // 1、创建获取文档请求
            GetRequest request = new GetRequest(
                    "mess",   //索引
                    "_doc",     // mapping type
                    "1");     //文档id

            // 2、可选的设置
            //request.routing("routing");
            //request.version(2);

            //request.fetchSourceContext(new FetchSourceContext(false)); //是否获取_source字段
            //选择返回的字段
            String[] includes = new String[]{"message", "*Date"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);
            SendRequest.sendRequest(request);

        }catch (IOException e) {
            e.printStackTrace();
        }

    }
}
