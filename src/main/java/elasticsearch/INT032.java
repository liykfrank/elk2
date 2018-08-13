package elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class INT032 {

    public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;
    public final static String START = "2018-06-06T15:59:59.000Z";
    public final static String END = "2018-06-08T15:59:59.000Z";

    TransportClient client;
    
	private static final String reportPath = "./src/main/resources/INT030_Report.csv";
	private static String path = "./src/main/resources/";

    /**
     * newgen_balance-alerts-service
     * 
     */
    private void init() throws UnknownHostException{
        // on startup
//    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(INT032.HOST),INT032.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    private void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws IOException {
    	
        INT032 es = new INT032();
        es.init();
        
        // CEST +02:00 中欧夏令时
        QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT032.START).to(INT032.END);
        
        
        //TODO: RHCAlert BalanceUpdate 
        QueryBuilder qbBalanceUpdateReceievedRHC = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"))
        		.must(QueryBuilders.matchPhraseQuery("message", "RHCAlert"));
        
        QueryBuilder qbBalanceUpdateReceievedBalance = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"))
        		.must(QueryBuilders.matchPhraseQuery("message", "BalanceUpdate"));
        
        
        QueryBuilder qbBalanceUpdateSuccess = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 202"));
        
        QueryBuilder qbBalanceUpdateFailed404 = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 404"));

        SearchResponse response4BalanceUpdateRHC = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateReceievedRHC)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4BalanceUpdateBalance = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateReceievedBalance)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4BalanceUpdateSuccess = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateSuccess)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4BalanceUpdateFailed404 = es.client.prepareSearch("newgen_balance-alerts-service")
        		.setTypes("log")
        		.setQuery(qbBalanceUpdateFailed404)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchHits hits4BalanceUpdateRHC = response4BalanceUpdateRHC.getHits();
        SearchHits hits4BalanceUpdateBalance = response4BalanceUpdateBalance.getHits();
        SearchHits hits4BalanceUpdateSuccess = response4BalanceUpdateSuccess.getHits();
        SearchHits hits4BalanceUpdateFailed404 = response4BalanceUpdateFailed404.getHits();
        
        // shades * size
        int pageNumUpdate1 = (int)hits4BalanceUpdateRHC.totalHits / (1 * 10);
        int pageNumUpdate2 = (int)hits4BalanceUpdateBalance.totalHits / (1 * 10);
        int pageNum404 = (int)hits4BalanceUpdateFailed404.totalHits / (1 * 10);
        
        // Get IDs from each hit
        String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
        String regx2 = "iataCode(.*?)storedRHCValue";
        Pattern pattern = Pattern.compile(regx);
        Pattern pattern2 = Pattern.compile(regx2);
        Matcher match = null;
        List<String> ids = new ArrayList<String>();
        List<String> ids404 = new ArrayList<String>();
        
        File report = new File(path+"INT032_RHCAlert_Report.csv");
        File report2 = new File(path+"INT032_BalanceUpdate_Report.csv");
        FileWriter fwr = new FileWriter(report);
        FileWriter fwr2 = new FileWriter(report2);
        
        fwr.write("ID,");
        fwr.write("IataCode,");
        fwr.write("Status");
        fwr.write("\r\n");
        
        fwr2.write("ID,");
        fwr2.write("IataCode,");
        fwr2.write("Status");
        fwr2.write("\r\n");
        
        
//		// get 404 failed ids
//		for(int i = 0; i <= pageNum404; i++) {
//			
////			System.out.println("------------------Page: " + i + "   Reason 404 ---------------------");
//			
//			for(SearchHit hit : response4BalanceUpdateFailed404.getHits()) {
//				match = pattern.matcher(hit.getSourceAsString());
//				if(match.find()) {
//					ids404.add(match.group(1));
////					System.out.println(match.group(1));
//				}
//			}
//			response4BalanceUpdateFailed404 = es.client.prepareSearchScroll(response4BalanceUpdateFailed404.getScrollId()).setScroll(new TimeValue(20000)).get();
//		}

        
        // get unknown failed ids
        for(int i = 0; i <= pageNumUpdate1; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4BalanceUpdateRHC.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
	        		
	        		fwr.write(match.group(1));
	        		fwr.write(",");
	        		
	        		QueryBuilder qb1 = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"))
	    	        		.must(qbTime);
	    			
	    			SearchResponse response1 = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qb1)
	    	        		.get();
	    			
	    			SearchHits hits1 = response1.getHits();
	    			
	    			for(SearchHit hit1 : response1.getHits()) {
	    	        	match = pattern2.matcher(hit1.getSourceAsString());
	    	        	if (match.find()) {
	    	        		String iataCode = match.group(1);
	    	        		System.out.println(iataCode);
	    	        		//TODO:
	    	        		String iataCode2 = iataCode.substring(6, 14);
	    	        		
	    	        		fwr.write(iataCode2);
	    	        		fwr.write(",");
	    	        	} else {
	    	        		fwr.write("not match");
	    	        		fwr.write(",");
	    	        	}
	    			}
	        		
	    			match = pattern.matcher(hit.getSourceAsString());
	    			match.find();
	        		
	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.boolQuery()
	    	        				.should(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 202")))
	    	        		.must(qbTime);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0) {
	    				ids.add(match.group(1));  	
	    				fwr.write("Failed");
	    				fwr.write("\r\n");
	    			} else {
	    				fwr.write("Success");
	    				fwr.write("\r\n");
	    			}
	    		}
	        }
	        response4BalanceUpdateRHC = es.client.prepareSearchScroll(response4BalanceUpdateRHC.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
     // get unknown failed ids
        for(int i = 0; i <= pageNumUpdate2; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4BalanceUpdateBalance.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
//    				System.out.println("ID: " + match.group(1));    			
//    				QueryBuilder qbID = QueryBuilders.matchPhraseQuery("message", match.group(1));
	        		
	        		fwr2.write(match.group(1));
	        		fwr2.write(",");
	        		
	        		QueryBuilder qb1 = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"))
	    	        		.must(qbTime);
	    			
	    			SearchResponse response1 = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qb1)
	    	        		.get();
	    			
	    			SearchHits hits1 = response1.getHits();
	    			
	    			for(SearchHit hit1 : response1.getHits()) {
	    	        	match = pattern2.matcher(hit1.getSourceAsString());
	    	        	if (match.find()) {
	    	        		String iataCode = match.group(1);
	    	        		System.out.println(iataCode);
	    	        		//TODO:
	    	        		String iataCode2 = iataCode.substring(6, 14);
	    	        		
	    	        		
	    	        		fwr2.write(iataCode2);
	    	        		fwr2.write(",");
	    	        	} else {
	    	        		fwr2.write("not match");
	    	        		fwr2.write(",");
	    	        	}
	    			}
	        		
	    			match = pattern.matcher(hit.getSourceAsString());
	    			match.find();
	    			
	    			// Query log by ID and is not known reason 
	    			QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.boolQuery()
	    	        				.should(QueryBuilders.matchPhraseQuery("message", "SFDC System API Response: HTTP Response Status: 202")))
	    	        		.must(qbTime);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match known
	    			if (hits4IDUnknown.getTotalHits() == 0){
	    				ids.add(match.group(1));  	
	    				fwr2.write("Failed");
	    				fwr2.write("\r\n");
	    			} else {
	    				fwr2.write("Success");
	    				fwr2.write("\r\n");
	    			}
	    		}
	        }
	        response4BalanceUpdateBalance = es.client.prepareSearchScroll(response4BalanceUpdateBalance.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
        
        
		File file = null;
		File file404 = null;
		FileWriter fw = null;
		Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dirName = sdf.format(now) + " " + now.getTime();
        String dirName404 = "404";
        
        file = new File(path + dirName);
        file404 = new File(path + dirName + "/" + dirName404);
        file.mkdirs();
        file404.mkdirs();
        
        // Query and Write Unknow Failed
        for (String id : ids) {
        	QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	// 对应的ID 日志不会很多 不用分页
        	SearchResponse response4BalanceUpdateUnknownFailed = es.client.prepareSearch("newgen*")
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
	 				System.out.println(hit.getSourceAsString());	 				
	 				fw.write(hit.getSourceAsString());
	 				fw.write("\r\n");
	 			}  
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				fw.close();
			}
        	 
         	System.out.println("---------------------------------Unknown Failed--------------------------------------------"); 	 
        }
        
        // Query and Write 404 Failed
//        for (String id : ids404) {
//        	QueryBuilder qbIDFailed404 = QueryBuilders.boolQuery()
//	        		.must(QueryBuilders.matchPhraseQuery("message", id))
//	        		.must(qbTime);
//        	
//        	 SearchResponse response4BalanceUpdateFailed404ById = es.client.prepareSearch("newgen*")
//             		.setTypes("log")
//             		.setQuery(qbIDFailed404)
//             		.setSize(10)
//             		.addSort("@timestamp", SortOrder.DESC)
//             		.get();
//        	 
//        	 SearchHits hits4BalanceUpdateFailed404ById = response4BalanceUpdateFailed404ById.getHits();
//        	 
//        	 file404 = new File(path + dirName + "/" + dirName404, id + ".txt");
//        	 try {
//				fw = new FileWriter(file404);
//				for(SearchHit hit : hits4BalanceUpdateFailed404ById) {
//	 				System.out.println(hit.getSourceAsString());	 				
//	 				fw.write(hit.getSourceAsString());
//	 				fw.write("\r\n");
//	 			}  
//				
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//				fw.close();
//			}
//        	 
//         	System.out.println("---------------------------------404 Failed----------------------------------------"); 	 
//        }
        
        System.out.println("Balance Update Hits Count: " + hits4BalanceUpdateRHC.totalHits + hits4BalanceUpdateBalance.totalHits);
        System.out.println("SUCCESS Count: " + hits4BalanceUpdateSuccess.totalHits);
        System.out.println("Total Failed Count: " + (ids.size() + ids404.size()));
        System.out.println("Unknow Failed Count: " + ids.size());
        System.out.println("404 Failed Count: " + ids404.size());
        
        fwr.close();
        fwr2.close();
        es.close();
    }
}
