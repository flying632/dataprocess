package com.neu.dataprocess.util;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author fengyuluo
 * @createTime 11:06 2019/4/23
 */
public class ClientUtil {
    private static RestHighLevelClient client;
    private static String clusterName = "elasticsearch";
    private static String serverHost = "120.25.57.198";
    private static int serverPort = 9200;
    public static RestHighLevelClient getClient(){
        if (client == null){
            return init();
        }
        return client;
    }

    private static RestHighLevelClient init(){
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(serverHost,serverPort,"http")
        );
        builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(HttpHost host) {
                //TODO 链接Elasticsearch失败，报警
                super.onFailure(host);
            }
        });
        client = new RestHighLevelClient(builder);
        System.out.println("Elasticsearch client 初始化成功");
        return client;
    }

}
