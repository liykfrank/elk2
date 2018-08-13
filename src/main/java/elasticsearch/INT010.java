package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class INT010 {
	
	public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;
    public final static String START = "2018-02-01T15:59:59.000Z";
    public final static String END = "2018-05-17T15:59:59.000Z";

    TransportClient client;

    /**
     * newgen_balance-alerts-service
     * 
     */
    private void init() throws UnknownHostException{
        // on startup
//    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(INT010.HOST),INT010.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    private void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws IOException {
    	
        INT010 es = new INT010();
        es.init();
        
        // CEST +02:00 中欧夏令时
        QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT010.START).to(INT010.END);
        
        QueryBuilder qb010Receieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "IEP account for external user"));
        
        QueryBuilder qbMSTSReceieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "IEP account for external user"))
        		.must(QueryBuilders.matchPhraseQuery("message", "IATA EasyPay (MSTS)"));
        
        QueryBuilder qbEDENREDReceieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "IEP account for external user"))
        		.must(QueryBuilders.matchPhraseQuery("message", "IATA EasyPay (EDENRED)"));
        
        QueryBuilder qb010Success = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.boolQuery()
        				.should(QueryBuilders.matchPhraseQuery("message", "POST User Provisioning Success!"))
        				.should(QueryBuilders.matchPhraseQuery("message", "PUT User Provisioning Success!"))
        				.should(QueryBuilders.matchPhraseQuery("message", "successfully removed from External System")));

        SearchResponse response4INT010 = es.client.prepareSearch("newgen_iata-provisioning-process")
        		.setTypes("log")
        		.setQuery(qb010Receieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4MSTS = es.client.prepareSearch("newgen_iata-provisioning-process")
        		.setTypes("log")
        		.setQuery(qbMSTSReceieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4EDENRED = es.client.prepareSearch("newgen_iata-provisioning-process")
        		.setTypes("log")
        		.setQuery(qbEDENREDReceieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4INT010Success = es.client.prepareSearch("newgen_iata-provisioning-process")
        		.setTypes("log")
        		.setQuery(qb010Success)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
       
        
        SearchHits hits4INT010 = response4INT010.getHits();
        SearchHits hits4INT010MSTS = response4MSTS.getHits();
        SearchHits hits4INT010EDENRED = response4EDENRED.getHits();
        SearchHits hits4INT010Success = response4INT010Success.getHits();
        
        // shades * size
        int pageNum = (int)hits4INT010.totalHits / (1 * 10);
        int pageNumMSTS = (int)hits4INT010MSTS.totalHits / (1 * 10);
        int pageNumEDENRED = (int)hits4INT010EDENRED.totalHits / (1 * 10);
        
        // Get IDs from each hit 
        String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
        Pattern pattern = Pattern.compile(regx);
        Matcher match = null;
        List<String> ids = new ArrayList<String>();
        List<String> idsMSTS = new ArrayList<String>();
        List<String> idsEDENRED = new ArrayList<String>();
        
     // get unknown failed ids
        for(int i = 0; i <= pageNum; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4INT010.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
//    				System.out.println("ID: " + match.group(1));    			
//    				QueryBuilder qbID = QueryBuilders.matchPhraseQuery("message", match.group(1));
	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(qb010Success);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_iata-provisioning-process")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0) {
//	    				System.out.println("Failed");
	    				ids.add(match.group(1));  	
	    			}
	    		}
	        }
	        response4INT010 = es.client.prepareSearchScroll(response4INT010.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
        // get unknown failed ids MSTS
        for(int i = 0; i <= pageNumMSTS; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4MSTS.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(qb010Success);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_iata-provisioning-process")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0)
	    				idsMSTS.add(match.group(1));  	
	    		}
	        }
	        response4MSTS = es.client.prepareSearchScroll(response4MSTS.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
     // get unknown failed ids EDENRED
        for(int i = 0; i <= pageNumEDENRED; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4EDENRED.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
//    				System.out.println("ID: " + match.group(1));    			
//    				QueryBuilder qbID = QueryBuilders.matchPhraseQuery("message", match.group(1));
	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(qb010Success);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_iata-provisioning-process")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0)
	    				idsEDENRED.add(match.group(1));  	
	    		}
	        }
	        response4EDENRED = es.client.prepareSearchScroll(response4EDENRED.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
                
        
		File file = null;
		FileWriter fw = null;
		Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dirName = "INT010 " + sdf.format(now) + " " + now.getTime();
        String path = "c:/Users/wangjm@iata.org/Desktop/";
        
        file = new File(path + dirName);
        file.mkdirs();
        
        // Query and Write Unknow Failed MSTS
        for (String id : ids) {
        	QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	// 对应的ID 日志不会很多 不用分页
        	SearchResponse response4BalanceUpdateUnknownFailed = es.client.prepareSearch("newgen_iata-provisioning-process")
             		.setTypes("log")
             		.setQuery(qbIDUnknowFailed)
             		.setSize(100)
             		.addSort("@timestamp", SortOrder.DESC)
             		.get();
        	 
        	 SearchHits hits4BalanceUpdateUnknownFailed = response4BalanceUpdateUnknownFailed.getHits();
        	 
        	 file = new File(path + dirName + "/", id + ".txt");
        	 try {
				fw = new FileWriter(file);
				for(SearchHit hit : hits4BalanceUpdateUnknownFailed) {
//	 				System.out.println(hit.getSourceAsString());	 				
	 				fw.write(hit.getSourceAsString());
	 				fw.write("\r\n");
	 			}  
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				fw.close();
			}
        	System.out.println(id);
         	System.out.println("---------------------------------Unknown Failed--------------------------------------------"); 	 
        }
        
        
        System.out.println("INT010 Receieved : " + hits4INT010.totalHits);
        System.out.println("INT010 MSTS Receieved : " + hits4INT010MSTS.totalHits);
        System.out.println("INT010 EDENRED Receieved : " + hits4INT010EDENRED.totalHits);
        System.out.println("********************************************************************************");
        System.out.println("Failed! Count: " + ids.size());
        System.out.println("MSTS Failed Count: " + idsMSTS.size());
        System.out.println("EDENRED Failed Count: " + idsEDENRED.size());
        System.out.println("********************************************************************************");
        System.out.println("Success! Count: " + hits4INT010Success.totalHits);
        System.out.println("MSTS Success! Count: " + (hits4INT010MSTS.totalHits - idsMSTS.size()));
        System.out.println("EDENRED Success! Count: " + (hits4INT010EDENRED.totalHits - idsEDENRED.size()));

        es.close();
        
    }

}
