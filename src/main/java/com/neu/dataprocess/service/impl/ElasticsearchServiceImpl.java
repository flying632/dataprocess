package com.neu.dataprocess.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.neu.dataprocess.entity.Host;
import com.neu.dataprocess.service.ElasticsearchService;
import com.neu.dataprocess.util.ClientUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.boot.jackson.JsonObjectDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @author fengyuluo
 * @createTime 10:20 2019/4/23
 */
public class ElasticsearchServiceImpl implements ElasticsearchService {
    private RestHighLevelClient client = ClientUtil.getClient();
    @Override
    public ArrayList<Host> getAllHosts() throws IOException {
        MatchAllQueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("hosts")
                .field("beat.hostname");
        TopHitsAggregationBuilder subAggregation = AggregationBuilders.topHits("hostInfo").size(1);
        aggregationBuilder.subAggregation(subAggregation);
        searchSourceBuilder.aggregation(aggregationBuilder);
//        searchSourceBuilder.profile(true);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);

        //解析返回值
        ArrayList<Host> hostArrayList = new ArrayList<>();
        String stringResponse = searchResponse.toString();
        JSONObject jsonObject = JSONObject.parseObject(stringResponse);
        JSONObject aggregations = (JSONObject)jsonObject.get("aggregations");
        JSONObject hosts = (JSONObject)aggregations.get("sterms#hosts");
        JSONArray buckets = (JSONArray)hosts.get("buckets");

        for (Object item : buckets){
            JSONObject object = (JSONObject)item;
            JSONObject hostInfo = (JSONObject)object.get("top_hits#hostInfo");
            JSONObject hits = (JSONObject)hostInfo.get("hits");
            JSONObject hitsFirst = (JSONObject)hits.getJSONArray("hits").get(0);
            JSONObject source = (JSONObject)hitsFirst.get("_source");
            JSONObject hostInforAns = (JSONObject)source.get("host");
            Host host = JSON.parseObject(hostInforAns.toString(),Host.class);
            hostArrayList.add(host);
        }
        for (Host host : hostArrayList)
            System.out.println(host.getName());

        return hostArrayList;
    }

    @Override
    public boolean diskIOCheck() {
        return false;
    }

    @Override
    public boolean memoryCheck() {
        return false;
    }

    @Override
    public boolean avaliableCheck() {
        return false;
    }
}
