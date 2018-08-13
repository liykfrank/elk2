package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class BalanceUpdateResponse {





	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-08-10T15:59:59.000Z";
	public final static String END = "2018-08-30T15:59:59.000Z";

	TransportClient client;

	/**
	 * newgen_rs-balances-service
	 * 
	 */
	private void init() throws UnknownHostException {
		// on startup
		// TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
		// Settings settings = Settings.builder().put("cluster.name", "name").build();
		client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(
				new TransportAddress(InetAddress.getByName(BalanceUpdateResponse.HOST), BalanceUpdateResponse.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		BalanceUpdateResponse es = new BalanceUpdateResponse();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(BalanceUpdateResponse.START).to(BalanceUpdateResponse.END);

		System.out.println("*********************************");
		
		File file = new File("c:/Users/wangjm@iata.org/Desktop/result.txt");
		FileWriter fw = new FileWriter(file);
		
		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "description(.*?)agencyId";
		String regx3 = "Status: (.*?),";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;


		QueryBuilder qbINCid = QueryBuilders.boolQuery()
				.must(qbTime)
//				.must(QueryBuilders.wildcardQuery("message", "192*"))
//				.must(QueryBuilders.matchPhraseQuery("message", "19201092"))
//				.must(QueryBuilders.matchPhraseQuery("message", "iataCode"))
				.must(QueryBuilders.matchPhraseQuery("message", "Response from IATA Event Service"));

		SearchResponse response4INC = es.client.prepareSearch("newgen_balance-alerts-service")
				.setTypes("log")
				.setQuery(qbINCid)
				.setSearchType(SearchType.DEFAULT)
				.setScroll(TimeValue.timeValueMinutes(7))
				.setSize(10)
				.addSort("@timestamp", SortOrder.DESC)
				.get();

		SearchHits hits4INC = response4INC.getHits();
		
		int pageNum = (int)hits4INC.totalHits / (1 * 10);
		
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		String id = null;
		String status = null;
		String description = null;

		 for(int i = 0; i <= pageNum; i++) {
			for (SearchHit hit :  response4INC.getHits()) {
				System.out.println(hit.getSourceAsString());
				
				List<String> status_reason = new ArrayList<String>();
				match = pattern.matcher(hit.getSourceAsString());
				if (match.find()) {
					id = match.group(1);
					System.out.println(id);
					fw.write(id);
					fw.write("    ");
				} else {
					fw.write("not match id");
					fw.write("    ");
				}
				
				match = pattern3.matcher(hit.getSourceAsString());
				if (match.find()) {
					status = match.group(1);
					System.out.println(status);
					status_reason.add(status);
					fw.write(status);
					fw.write("    ");
				} else {
					status_reason.add("not match status");
					fw.write("not match status");
					fw.write("    ");
				}
				
				match = pattern2.matcher(hit.getSourceAsString());
				if (match.find()) {
					String desc = match.group(1);
					description = desc.substring(5, desc.length() - 5);
					System.out.println(description);
					status_reason.add(description);
					fw.write(description);
					fw.write("    ");
				} else {
					status_reason.add("not match description");
					fw.write("not match description");
					fw.write("    ");
				}
				
				fw.write("\r\n");
				
				map.put(id, status_reason);	
				
			}
			response4INC = es.client.prepareSearchScroll(response4INC.getScrollId()).setScroll(new TimeValue(20000)).get();
		 }
		 
		 fw.close();
		 
		 // find specific result
		 List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/ids.txt"));
		 File file2 = new File("c:/Users/wangjm@iata.org/Desktop/status.csv");
		 FileWriter fw2 = new FileWriter(file2);
		 for (String line : lines) {
			 fw2.write(line);
			 fw2.write(",");
			 if(map.containsKey(line)) {
				 fw2.write(map.get(line).get(0));
				 fw2.write(",");
				 fw2.write(map.get(line).get(1));
			 } else {
				 fw2.write("No Response");
			 }
			 fw2.write("\r\n");
		 }
		 
		 fw2.close();	
		
		es.close();
	}











}
