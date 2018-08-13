package elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class ElasticsearchTest1 {

    public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;

    TransportClient client;

    /**
     * 
     * 
     */
    public void init() throws UnknownHostException{
        // on startup
    	
    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
    	
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
    	
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(ElasticsearchTest1.HOST),ElasticsearchTest1.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    public void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws UnknownHostException {
    	
    	
        ElasticsearchTest1 es = new ElasticsearchTest1();
        es.init();

//        GetResponse getResponse = es.client.prepareGet("newgen_featurespace-system-service", "log", "3612138284").get();
//        System.out.println("***************************** Source *****************************");
//        System.out.println(getResponse.getSourceAsString());
//        System.out.println("***************************** Source *****************************");
//        
//        System.out.println("***********************************************************");
//        System.out.println(getResponse.getIndex());
//        System.out.println(getResponse.getType());
//        System.out.println(getResponse.getSourceAsMap());
        
        
        
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        QueryBuilder qb2 = QueryBuilders.rangeQuery("@timestamp").gte("now-240h");
        
        QueryBuilder qbfrom = QueryBuilders.rangeQuery("@timestamp").gte("2018-03-30T23:59:59.000Z").includeLower(true);
        QueryBuilder qbto = QueryBuilders.rangeQuery("@timestamp").lte("2018-04-30T23:59:59.999Z").includeUpper(true);
        
        QueryBuilder qbfrom1 = QueryBuilders.rangeQuery("@timestamp").from("2018-03-31T15:59:59.000Z").to("2018-04-30T15:59:59.999Z");
        
        // April 24th 2018, 09:59:45.759
//        CEST +02:00 中欧夏令时
        QueryBuilder qb4 = QueryBuilders.matchPhraseQuery("message", "RS-Balances routing to matched  IEP Vendor - EDENRED");
        QueryBuilder qb5 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("tags", "_grokparsefailure"));
        QueryBuilder qb6 = QueryBuilders.boolQuery()
        		.should(QueryBuilders.matchPhraseQuery("message", "RS-Balances request has no EasyPay Vendor specified, so routing to Featurespace (RME)"))
        		.should(QueryBuilders.matchPhraseQuery("message", "RS-Balances routing to matched  IEP Vendor - MSTS"))
        		.should(QueryBuilders.matchPhraseQuery("message", "RS-Balances routing to matched  IEP Vendor - EDENRED"));
        
        QueryBuilder qbb = QueryBuilders.boolQuery()
        		.must(qbfrom1)
//        		.must(qbto)
        		.must(qb6);
        
        
        
        SearchResponse searchResponse = es.client.prepareSearch("newgen_featurespace-system-service")
        		.setTypes("log")
        		.setQuery(qb)
        		.setFrom(0).setSize(2)
        		.get();
        
        SearchHits hits = searchResponse.getHits();
        System.out.println("Hits Count: " + hits.totalHits);
        
//        for(SearchHit hit : hits) {
//        	System.out.println(hit.getSourceAsString());
//        	System.out.println("********************************************************************************");
//        }
        
        
        
        
        
        SearchResponse response4rsBalance = es.client.prepareSearch("newgen_rs-balances*")
        		.setTypes("log")
        		.setQuery(qbb)
//        		.setQuery(qbto)
        		.addSort("@timestamp", SortOrder.DESC)
        		.setFrom(0).setSize(100)
        		.get();
        
        SearchHits hits4rsBalance = response4rsBalance.getHits();
        
//        DateFormat dateFormat = new SimpleDateFormat("Z");
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("Z");
        System.out.println(dateFormat.format(new Date()));
        
        System.out.println("RS Balance Hits Count: " + hits4rsBalance.totalHits);
        
        
        for(SearchHit hit : hits4rsBalance) {
        	System.out.println(hit.getSourceAsString());
        	System.out.println();
        	System.out.println("-----------------------------------------------------------------------------------");
        }

        es.close();
    }
}
