package com.womaiapp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ResourceBundle;
import java.util.Stack;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EsDeleteByType {

    private static final Logger logger = LoggerFactory.getLogger(EsDeleteByType.class);
    private Client client;

    private static ResourceBundle getEsConfig(){
        return ResourceBundle.getBundle("es");
    }

    private void getClient(){
        String clusterName = getEsConfig().getString("clusterName");
        String hosts = getEsConfig().getString("hosts");
        if (hosts == null || clusterName == null) {
            throw new IllegalArgumentException("hosts or clusterName was null.");
        }
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
        client = TransportClient.builder()
                .addPlugin(DeleteByQueryPlugin.class)
                .settings(settings).build();
        String[] hostsArray = hosts.split(",");
        for(String hostAndPort : hostsArray){
            String[] tmpArray = hostAndPort.split(":");
            try {
                client = ((TransportClient)client).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(tmpArray[0]), Integer.valueOf(tmpArray[1])));
            } catch (NumberFormatException e) {
                logger.error(e.getMessage());
            } catch (UnknownHostException e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * 判断一个index中的type是否有数据 
     * @param index
     * @param type
     * @return
     * @throws Exception
     */
    public Boolean existDocOfType(String index, String type) throws Exception {
        SearchRequestBuilder builder = client.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(1);
        SearchResponse response = builder.execute().actionGet();
        long docNum = response.getHits().getTotalHits();
        if (docNum == 0) {
            return false;
        }
        return true;
    }

    /**
     * 根据type来删除数据 
     * @param index
     * @param types
     * @return
     */
    public long deleteDocByType(String index, String[] types) {
        getClient();
        long oldTime = System.currentTimeMillis();
        StringBuilder b = new StringBuilder();
        b.append("{\"query\":{\"match_all\":{}}}");
        DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .setIndices(index).setTypes(types)
                .setSource(b.toString())
                .execute().actionGet();
        Stack<String> allTypes = new Stack<String>();
        for(String type : types){
            allTypes.add(type);
        }
        while(!allTypes.isEmpty()){
            String type = allTypes.pop();
            while(true){
                try {
                    if (existDocOfType(index, type) == false) {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("queryError: " + e.getMessage());
                }
            }
        }
        System.out.println(System.currentTimeMillis() - oldTime);
        return response.getTotalDeleted();
    }

    //删除操作，单个删除
    public void deleteByTerm(Client client){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        SearchResponse response = client.prepareSearch("logstash-tinker-test-2017.05.10").setTypes("tinker-test")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("type", "std"))
                .setFrom(0).setSize(20).setExplain(true).execute().actionGet();
        for(SearchHit hit : response.getHits()){
            String id = hit.getId();
            bulkRequest.add(client.prepareDelete("logstash-tinker-test-2017.05.10", "tinker-test", id).request());
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for(BulkItemResponse item : bulkResponse.getItems()){
                System.out.println(item.getFailureMessage());
            }
        }else {
            System.out.println("delete ok");
        }

    }
    public static void main(String[] args) {
        EsDeleteByType es=new EsDeleteByType();
        es.getClient();
        if(es.client!=null){
            es.deleteByTerm(es.client);
        }
    }
}  