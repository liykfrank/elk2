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

public class INT068 {
	
    public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;
    public final static String START = "2018-06-06T15:59:59.000Z";
    public final static String END = "2018-06-08T15:59:59.000Z";
	private static final String reportPath = "./src/main/resources/INT068_ConsumedRHC_Report.csv";
	private static final String path = "./src/main/resources/";;

    TransportClient client;

    /**
     * newgen_iata-agencies-process
     * 
     */
    private void init() throws UnknownHostException{
        // on startup
//    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(INT068.HOST),INT068.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    private void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws IOException {
    	
        INT068 es = new INT068();
        es.init();
        
        // CEST +02:00 中欧夏令时
        QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT068.START).to(INT068.END);
        
        QueryBuilder qbINT068Receieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "Received Consumed Provisional RHC Request for agency"));
        
        QueryBuilder qbINT068Success = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "INT_068 Response Sent to SFDC"));
        
        SearchResponse response4INT068 = es.client.prepareSearch("newgen_iata-agencies-process")
        		.setTypes("log")
        		.setQuery(qbINT068Receieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4INT068eSuccess = es.client.prepareSearch("newgen_iata-agencies-process")
        		.setTypes("log")
        		.setQuery(qbINT068Success)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchHits hits4INT068 = response4INT068.getHits();
        SearchHits hits4INT068Success = response4INT068eSuccess.getHits();
        
        File report = new File(reportPath);
        FileWriter fwr = new FileWriter(report);
        fwr.write("ID,");
        fwr.write("IataCode,");
        fwr.write("Status");
        fwr.write("\r\n");
        
        // shades * size
        int pageNum068 = (int)hits4INT068.totalHits / (1 * 10);
        int pageNumSuccess = (int)hits4INT068Success.totalHits / (1 * 10);
        
        // Get IDs from each hit
        String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
        String regx2 = "agency: (.*?)path";
        Pattern pattern = Pattern.compile(regx);
        Pattern pattern2 = Pattern.compile(regx2);
        Matcher match = null;
        List<String> ids = new ArrayList<String>();
        
        // get unknown failed ids
		long startTime = System.currentTimeMillis();
        for(int i = 0; i <= pageNum068; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4INT068.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
//    				System.out.println("ID: " + match.group(1));    			
//    				QueryBuilder qbID = QueryBuilders.matchPhraseQuery("message", match.group(1));
	    			
	        		fwr.write(match.group(1));
	        		fwr.write(",");
	        		
	        		QueryBuilder qb1 = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.matchPhraseQuery("message", "Received Consumed Provisional RHC Request for agency:"))
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
	    	        		String iataCode2 = iataCode.substring(0, 8);
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
	    	        		.must(QueryBuilders.matchPhraseQuery("message", "INT_068 Response Sent to SFDC"))
	    	        		.must(qbTime);
	    			
	    			SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
	    			// if don't match success
	    			if (hits4IDUnknown.getTotalHits() == 0){
	    				ids.add(match.group(1));  	
	    				fwr.write("Failed");
	    				fwr.write("\r\n");
	    			} else {
	    				fwr.write("Success");
	    				fwr.write("\r\n");
	    			}
	    		}
	        }
	        response4INT068 = es.client.prepareSearchScroll(response4INT068.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
        long endTime = System.currentTimeMillis();
        
        
		File file = null;
		FileWriter fw = null;
		Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dirName = "INT068 " + sdf.format(now) + " " + now.getTime();
       
        
        file = new File(path + dirName);
        file.mkdirs();
        
        // Query and Write Unknow Failed
        for (String id : ids) {
        	QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	// 对应的ID 日志不会很多 不用分页
        	SearchResponse response4INT068UnknownFailed = es.client.prepareSearch("newgen*")
             		.setTypes("log")
             		.setQuery(qbIDUnknowFailed)
             		.setSize(100)
             		.addSort("@timestamp", SortOrder.DESC)
             		.get();
        	 
        	 SearchHits hits4INT068UnknownFailed = response4INT068UnknownFailed.getHits();
        	 
        	 file = new File(path + dirName + "/", id + ".txt");
        	 try {
				fw = new FileWriter(file);
				for(SearchHit hit : hits4INT068UnknownFailed) {
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
   
        System.out.println("INT068 Hits Count: " + hits4INT068.totalHits);
        System.out.println("SUCCESS Count: " + hits4INT068Success.totalHits);
        System.out.println("Total Failed Count: " + (ids.size()));
        System.out.println("Query "+ hits4INT068.totalHits + " Run Time: " + (endTime - startTime) / 1000 + "s");
        
        fwr.close();
        es.close();      
    }

}
