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

public class INT031 {
	
    public final static String HOST = "10.140.8.212";
    public final static int PORT = 9300;
    public final static String START = "2018-06-04T15:59:59.000Z";
    public final static String END = "2018-06-05T15:59:59.000Z";

    TransportClient client;

    /**
     * newgen_rs-calendar-service
     * 
     */
    private void init() throws UnknownHostException{
        // on startup
//    	TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2")); 
//    	Settings settings = Settings.builder().put("cluster.name", "name").build();
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(INT031.HOST),INT031.PORT));
        System.out.println("Elasticsearch connect info: " + client.nodeName());
    }
    
    private void close() {
        // on shutdown
        client.close();
    }

    public static void main(String[] args) throws IOException {
    	
        INT031 es = new INT031();
        es.init();
        
        // CEST +02:00 中欧夏令时
        QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT031.START).to(INT031.END);
        
        
        // RS-Calendar request Payload
        QueryBuilder qbINT031Receieved = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "RS-Calendar request Payload"));
        
        // ID
        // SFTP Outbound Response
        // "code": "200"		
        QueryBuilder qbINT031Success = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "SFTP Outbound Response"))
        		.must(QueryBuilders.matchPhraseQuery("message", "\"code\": \"200\""));

        SearchResponse response4INT031 = es.client.prepareSearch("newgen_rs-calendar-service")
        		.setTypes("log")
        		.setQuery(qbINT031Receieved)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        SearchResponse response4INT031Success = es.client.prepareSearch("newgen_rs-calendar-service")
        		.setTypes("log")
        		.setQuery(qbINT031Success)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
        
        
        SearchHits hits4INT031 = response4INT031.getHits();
        SearchHits hits4INT031Success = response4INT031Success.getHits();
        
        // shades * size
        int pageNum = (int)hits4INT031.totalHits / (1 * 10);
        
        File report = new File("c:/Users/wangjm@iata.org/Desktop/INT031_Report.csv");
        FileWriter fwr = new FileWriter(report);
        
        // Get IDs from each hit
        String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
        String regx2 = "<strOperationCode>(.*?)</strOperationCode>";
        Pattern pattern = Pattern.compile(regx);
        Pattern pattern2 = Pattern.compile(regx2);
        Matcher match = null;
        List<String> ids = new ArrayList<String>();
        
        // get unknown failed ids
		long startTime = System.currentTimeMillis();
        for(int i = 0; i <= pageNum; i++) {
        	
//        	System.out.println("------------------Page: " + i + "   Reason Unknown ---------------------");
	        for(SearchHit hit : response4INT031.getHits()) {
//        		System.out.println(hit.getSourceAsString());
	        	match = pattern.matcher(hit.getSourceAsString());
	        	// query log by ID
	        	if(match.find()) {
	        		
	        		fwr.write(match.group(1));
	        		fwr.write(",");

	        		QueryBuilder qb1 = QueryBuilders.boolQuery()
	    	        		.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
	    	        		.must(QueryBuilders.matchPhraseQuery("message", "RS-Calendar file content post formatting"))
	    	        		.must(qbTime);
	    			
	    			SearchResponse response1 = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qb1)
	    	        		.get();
	    			
	    			SearchHits hits1 = response1.getHits();
	    			
	    			for(SearchHit hit1 : response1.getHits()) {
	    	        	match = pattern2.matcher(hit1.getSourceAsString());
	    	        	if (match.find()) {
	    	        		fwr.write(match.group(1));
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
	    	        		.must(qbINT031Success)
	    	        		.must(qbTime);
	    			
	    			SearchResponse responseINT031Known = es.client.prepareSearch("newgen*")
	    	        		.setTypes("log")
	    	        		.setQuery(qbIDUnknow)
	    	        		.get();
	    			
	    			SearchHits hits4IDUnknown = responseINT031Known.getHits();
	    			// if don't match known
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
	        response4INT031 = es.client.prepareSearchScroll(response4INT031.getScrollId()).setScroll(new TimeValue(20000)).get();
        }
        
        long endTime = System.currentTimeMillis();
        
        
		File file = null;
		FileWriter fw = null;
		Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dirName = "INT031 " + sdf.format(now) + " " + now.getTime();
        String path = "c:/Users/wangjm@iata.org/Desktop/";
        
        file = new File(path + dirName);
        file.mkdirs();
        
        // Query and Write Unknow Failed
        for (String id : ids) {
        	QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
	        		.must(QueryBuilders.matchPhraseQuery("message", id))
	        		.must(qbTime);
        	// 对应的ID 日志不会很多 不用分页
        	SearchResponse response4INT031UnknownFailed = es.client.prepareSearch("newgen*")
             		.setTypes("log")
             		.setQuery(qbIDUnknowFailed)
             		.setSize(100)
             		.addSort("@timestamp", SortOrder.DESC)
             		.get();
        	 
        	 SearchHits hits4INT031UnknownFailed = response4INT031UnknownFailed.getHits();
        	 
        	 file = new File(path + dirName + "/", id + ".txt");
        	 try {
				fw = new FileWriter(file);
				for(SearchHit hit : hits4INT031UnknownFailed) {
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
        
        System.out.println("INT031 Hits Count: " + hits4INT031.totalHits);
        System.out.println("SUCCESS Count: " + hits4INT031Success.totalHits);
        System.out.println("Total Failed Count: " + (ids.size()));
        System.out.println("Query "+ hits4INT031.totalHits + " Run Time: " + (endTime - startTime) / 1000 + "s");
        
        fwr.close();
        es.close();
        
    }

}
