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

public class BalanceUpdatePayload {




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
				new TransportAddress(InetAddress.getByName(BalanceUpdatePayload.HOST), BalanceUpdatePayload.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		BalanceUpdatePayload es = new BalanceUpdatePayload();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(BalanceUpdatePayload.START).to(BalanceUpdatePayload.END);

		System.out.println("*********************************");
		
		File file = new File("c:/Users/wangjm@iata.org/Desktop/result.txt");
		FileWriter fw = new FileWriter(file);
		
		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "]: (.*?)}";
		String regx3 = "iataCode(.*?)storedRHCValue";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;


		QueryBuilder qbINCid = QueryBuilders.boolQuery()
				.must(qbTime)
//				.must(QueryBuilders.wildcardQuery("message", "192*"))
//				.must(QueryBuilders.matchPhraseQuery("message", "19201092"))
//				.must(QueryBuilders.matchPhraseQuery("message", "iataCode"))
				.must(QueryBuilders.matchPhraseQuery("message", "BalanceUpdate"))
				.must(QueryBuilders.matchPhraseQuery("message", "Incoming Payload"));

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
		
		List<String> listCode = new ArrayList<String>();
		
		// find all alert
		 for(int i = 0; i <= pageNum; i++) {
			for (SearchHit hit :  response4INC.getHits()) {
				System.out.println(hit.getSourceAsString());
				
//				match = pattern3.matcher(hit.getSourceAsString());
//				if (match.find()) {
//					String code = match.group(1).substring(6, match.group(1).length() - 9);
//					fw.write(code);
//					fw.write("    ");
//				} else {
//					fw.write("not match iata code");
//					fw.write("    ");
//				}
//				
				match = pattern2.matcher(hit.getSourceAsString());
				if (match.find()) {
					String payload = match.group(1) + "}";
					System.out.println(payload);
					fw.write(payload);
					fw.write("\r\n");
				} else {
					fw.write("not match time");
					fw.write("\r\n");
				}
				
				
			}
			response4INC = es.client.prepareSearchScroll(response4INC.getScrollId()).setScroll(new TimeValue(20000)).get();
		 }
		 
		 fw.close();
		 
//		 // find specific result
//		 List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/result.txt"));
//		 File file2 = new File("c:/Users/wangjm@iata.org/Desktop/specResult.txt");
//		 FileWriter fw2 = new FileWriter(file2);
//		 for (String line : lines) {
//			 String digital = line.substring(0, 3);
//			 if(digital.equals("192")) {
//				 fw2.write(line);
//				 fw2.write("\r\n");
//			 }
//			 
//		 }
//		 
//		 fw2.close();	
		
		es.close();
	}









}
