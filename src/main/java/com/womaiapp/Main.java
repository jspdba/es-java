package com.womaiapp;



import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class Main {

    private static String ServerIP = "10.6.27.122";// ElasticSearch server ip
    private static int ServerPort = 9300;// port
    private Client client;

    public static void main(String[] args) {

        try {
            Client client = TransportClient.builder().build().addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName("10.6.27.122"), 9300));

//            QueryBuilder qb1 = termQuery("DEVICEID", "*");
            Main es =new Main();
//            es.testSearch(client,"logstash-tinker-prod-2017.05.10","tinker-prod");
            es.queryAndDelete(client,"logstash-tinker-prod-2017.05.10","tinker-prod");

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

//        deleteIndex("test");//删除名为test的索引库
    }

    public void testSearch(Client transportClient,String index,String type){
        SearchResponse searchResponse = transportClient.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.wildcardQuery("DEVICEID","*"))
//                .setQuery(QueryBuilders.matchAllQuery()) //查询所有

//                .setQuery(QueryBuilders.matchQuery("DEVICEID", "\\d+").operator(MatchQueryBuilder.Operator.OR)) //根据tom分词查询name,默认or
//                .setQuery(QueryBuilders.multiMatchQuery("", "DEVICEID")) //指定查询的字段
//                .setQuery(QueryBuilders.queryStringQuery("name:to* AND age:[0 TO 19]")) //根据条件查询,支持通配符大于等于0小于等于19
//                .setQuery(QueryBuilders.queryStringQuery("DEVICEID:\\d+")) //根据条件查询,支持通配符大于等于0小于等于19
//                .setQuery(QueryBuilders.termQuery("DEVICEID", "*"))//查询时不分词
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(0).setSize(10)//分页
//                .addSort("age", SortOrder.DESC)//排序
                .get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        System.out.println(total);
        SearchHit[] searchHits = hits.hits();
        for(SearchHit s : searchHits){
            System.out.println(s.getId());
            System.out.println(s.getSourceAsString());
        }
    }


    public void queryAndDelete(Client transportClient,String index,String type){
        SearchResponse searchResponse = transportClient.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.wildcardQuery("DEVICEID","868014023876966"))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(0).setSize(10)//分页
                .get();
        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        System.out.println(total);

        SearchHit[] searchHits = hits.hits();


        for(SearchHit s : searchHits){
            DeleteResponse dResponse = transportClient.prepareDelete("logstash-tinker-prod-2017.05.10", "tinker-prod", s.getId()).execute().actionGet();

            if(dResponse==null){
                System.out.println("dResponse null="+s.getId());
                continue;
            }

            if (dResponse.isFound()) {
                System.out.println("删除成功");
            } else {
                System.out.println("删除失败");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // 删除索引库

    public static void deleteIndex(String indexName) {

        try {
            if (!isIndexExists(indexName)) {
                System.out.println(indexName + " not exists");
            } else {
                Client client = TransportClient.builder().build().addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(ServerIP),
                                ServerPort));

                DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
                        .execute().actionGet();
                if (dResponse.isAcknowledged()) {
                    System.out.println("delete index "+indexName+"  successfully!");
                }else{
                    System.out.println("Fail to delete index "+indexName);
                }
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    // 创建索引库
    public static void createIndex(String indexName) {
        try {
            Client client = TransportClient.builder().build().addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(ServerIP), ServerPort));

            // 创建索引库

            if (isIndexExists("indexName")) {
                System.out.println("Index  " + indexName + " already exits!");
            } else {
                CreateIndexRequest cIndexRequest = new CreateIndexRequest("indexName");
                CreateIndexResponse cIndexResponse = client.admin().indices().create(cIndexRequest)
                        .actionGet();
                if (cIndexResponse.isAcknowledged()) {
                    System.out.println("create index successfully！");
                } else {
                    System.out.println("Fail to create index!");
                }

            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    // 判断索引是否存在 传入参数为索引库名称
    public static boolean isIndexExists(String indexName) {
        boolean flag = false;
        try {
            Client client = TransportClient.builder().build().addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(ServerIP), ServerPort));

            IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

            IndicesExistsResponse inExistsResponse = client.admin().indices()
                    .exists(inExistsRequest).actionGet();

            if (inExistsResponse.isExists()) {
                flag = true;
            } else {
                flag = false;
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return flag;
    }

}