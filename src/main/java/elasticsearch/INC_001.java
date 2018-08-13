package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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

public class INC_001 {
	



	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-02-28T15:59:59.000Z";
	public final static String END = "2018-03-11T15:59:59.000Z";

	TransportClient client;

	private void init() throws UnknownHostException {
		// on startup
		// TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
		// Settings settings = Settings.builder().put("cluster.name", "name").build();
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName(INC_001.HOST), INC_001.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		INC_001 es = new INC_001();
		es.init();

		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INC_001.START).to(INC_001.END);

		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "number(.*?)u_leading_";
		String regx3 = "@timestamp\":\"(.*?)\"";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;

		QueryBuilder qb = QueryBuilders.boolQuery()
        		.must(qbTime)
        		.must(QueryBuilders.matchPhraseQuery("message", "Network connection timed-out or response incorrect from BSPLink"));
		
		SearchResponse response = es.client.prepareSearch("newgen*")
        		.setTypes("log")
        		.setQuery(qb)
        		.setSearchType(SearchType.DEFAULT)
        		.setScroll(TimeValue.timeValueMinutes(7))
        		.setSize(10)
        		.addSort("@timestamp", SortOrder.DESC)
        		.get();
		
		SearchHits hits = response.getHits();
		
		// shades * size
        int pageNum = (int)hits.totalHits / (1 * 10);
        List<String> ids = new ArrayList<String>();
        List<String> numbers = new ArrayList<String>();
        
        File file1 = new File("c://Users/wangjm@iata.org/Desktop/" + "INC_001.txt");
		file1.createNewFile();
		FileWriter fw1 = new FileWriter(file1);
        
        // get failed ids
 		for(int i = 0; i <= pageNum; i++) {
 			
 			for(SearchHit hit : response.getHits()) {
 				match = pattern.matcher(hit.getSourceAsString());
 				if(match.find()) {
 					ids.add(match.group(1));
 					System.out.println(match.group(1));
 				}
 			}
 			response = es.client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(20000)).get();
 		}
 		
 		System.out.println("**************************************************************************************");
 		
 		for (String id : ids) {
 			
 			QueryBuilder qb1 = QueryBuilders.boolQuery()
 	        		.must(qbTime)
 	        		.must(QueryBuilders.matchPhraseQuery("message", id))
 	        		.must(QueryBuilders.matchPhraseQuery("message", "Successfully created incident, response received"));
 			
 			SearchResponse response1 = es.client.prepareSearch("newgen*")
 	        		.setTypes("log")
 	        		.setQuery(qb1)
 	        		.addSort("@timestamp", SortOrder.DESC)
 	        		.get();
 			
 			SearchHits hits1 = response1.getHits();
 			
 			for(SearchHit hit : response1.getHits()) {
// 				System.out.println(hit.getSourceAsString());
 				match = pattern2.matcher(hit.getSourceAsString());
 				if(match.find()) {
// 					numbers.e
 					fw1.write(match.group(1));
 					fw1.write("\r\n");
 				}
 			}
 		}
     		
	
		System.out.println("Network connection timed-out or response incorrect from BSPLink : " + hits.totalHits);
		System.out.println("Numbers : " + numbers.size());
		
//		System.out.println("Successfully created incident, response received" + hits1.totalHits);
		es.close();
		fw1.close();
	}

}
