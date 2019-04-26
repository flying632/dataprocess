package com.neu.dataprocess.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.neu.dataprocess.entity.Host;
import com.neu.dataprocess.service.ElasticsearchService;
import com.neu.dataprocess.util.ClientUtil;
import com.neu.dataprocess.util.MailUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.mail.MessagingException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author fengyuluo
 * @createtime 10:20 2019/4/23
 */
public class ElasticsearchServiceImpl implements ElasticsearchService {
    private RestHighLevelClient client = ClientUtil.getClient();
    private ArrayList<Host> hostArrayList = new ArrayList<>();
    String to = "ba_632@163.com";
    @Override
    public ArrayList<Host> getAllHosts() throws IOException {
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
    public void diskCheck() throws IOException, MessagingException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        for (Host host : hostArrayList) {
            diskCheckCore(host.getName());
        }
    }
    private void diskCheckCore(String hostname) throws IOException, MessagingException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //15分钟之内
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp");
//        rangeQueryBuilder.gte("now-1m");
        rangeQueryBuilder.lte("now");
        //硬盘容量占用在90%以上
        RangeQueryBuilder diskRange = QueryBuilders.rangeQuery("system.filesystem.used.pct");
        diskRange.gte(0);
        diskRange.lt(90);
        //主机名
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("host.name",hostname);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);
        boolQueryBuilder.filter(diskRange);

        //根据硬盘分区不同聚合
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("diskUsed").field("system.filesystem.device_name");

        searchSourceBuilder.size(0);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        JSONObject jsonResponse = JSONObject.parseObject(searchResponse.toString());
        JSONObject jsonAggregation = (JSONObject)jsonResponse.get("aggregations");
        JSONObject jsonfileUsed = (JSONObject)jsonAggregation.get("sterms#diskUsed");
        JSONArray jsonbuckets = jsonfileUsed.getJSONArray("buckets");
        if (jsonbuckets.size() != 0) {
            for (Object item : jsonbuckets) {
                JSONObject jsonItem = (JSONObject)item;
                String device_name = (String)jsonItem.get("key");
                //告警推送
                MailUtil.sendMail(to,"硬盘容量告警",device_name+"的DISK占用比高于90%");
//                System.out.println(hostname+":"+device_name+"的DISK占用比高于90%");
            }
        }
    }

    @Override
    public void cpuCheck() throws IOException, MessagingException {
        for (Host host : hostArrayList) {
            boolean result = cpuCheckCore(host.getName());
            //告警推送
            if (result){
                MailUtil.sendMail(to,"CPU负载告警",host.getName()+"的CPU负载过高");
//                System.out.println(host.getName()+"的CPU负载过高");
            }
        }
    }

    private boolean cpuCheckCore(String hostname) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //15分钟之内
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp");
        rangeQueryBuilder.gte("now-15m");
        rangeQueryBuilder.lte("now");
        //cpu负载在95%以上
        RangeQueryBuilder cpuRange = QueryBuilders.rangeQuery("system.cpu.total.pct");
        cpuRange.lt(95);
        //主机名
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("host.name",hostname);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);
        boolQueryBuilder.filter(cpuRange);

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        long totalHits = searchResponse.getHits().totalHits;
        if (totalHits == 0)
            return true;
        else
            return false;
    }

    @Override
    public void memoryCheck() throws IOException, MessagingException {
        for (Host host : hostArrayList) {
            boolean result = memoryCheckCore(host.getName());
            //告警推送
            if (result){
                MailUtil.sendMail(to,"内存负载告警",host.getName()+"的内存负载过高");
//                System.out.println(host.getName()+"的内存负载过高");
            }
        }

    }
    private boolean memoryCheckCore(String hostname) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //15分钟之内
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp");
        rangeQueryBuilder.gte("now-15m");
        rangeQueryBuilder.lte("now");
        //内存负载在95%以上
        RangeQueryBuilder memoryRange = QueryBuilders.rangeQuery("system.memory.used.pct");
        memoryRange.lt(95);
        //主机名
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("host.name",hostname);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);
        boolQueryBuilder.filter(memoryRange);

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        long totalHits = searchResponse.getHits().totalHits;
        if (totalHits == 0)
            return true;
        else
            return false;
    }

    @Override
    public void availableCheck() throws IOException, MessagingException {
        for (Host host : hostArrayList) {
            boolean result = availableCheckCore(host.getName());
            if (!result) {
                MailUtil.sendMail(to,"连接状态",host.getName()+"失联");
//                System.out.println(host.getName()+"失联");
            }
        }

    }
    private boolean availableCheckCore(String hostname) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("host.name",hostname));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("@timestamp").gt("now-1m"));
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        long totalHits = searchResponse.getHits().totalHits;
        if (totalHits == 0) {
            return false;
        } else {
            return true;
        }
    }
}
