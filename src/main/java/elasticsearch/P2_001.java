package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

public class P2_001 {

	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-05-15T15:59:59.000Z";
	public final static String END = "2018-06-01T15:59:59.000Z";

	TransportClient client;

	/**
	 * newgen_balance-alerts-service
	 * 
	 */
	private void init() throws UnknownHostException {
		// on startup
		// TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
		// Settings settings = Settings.builder().put("cluster.name", "name").build();
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName(P2_001.HOST), P2_001.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		P2_001 es = new P2_001();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(P2_001.START).to(P2_001.END);

		QueryBuilder qbBalanceUpdateReceieved = QueryBuilders.boolQuery().must(qbTime)
				.must(QueryBuilders.matchPhraseQuery("message", "Balance Alerts Incoming Payload:"))
				.must(QueryBuilders.matchPhraseQuery("message", "BalanceUpdate"));

		SearchResponse response4BalanceUpdate = es.client.prepareSearch("newgen_balance-alerts-service").setTypes("log")
				.setQuery(qbBalanceUpdateReceieved).setScroll(TimeValue.timeValueMinutes(7)).setSize(10)
				.addSort("@timestamp", SortOrder.ASC).get();

		SearchHits hits4BalanceUpdate = response4BalanceUpdate.getHits();

		// shades * size
		int pageNumUpdate = (int) hits4BalanceUpdate.totalHits / (1 * 10);

		System.out.println(hits4BalanceUpdate.totalHits);
		System.out.println(pageNumUpdate);

		// Get iata code from each hit
		// String regx =
		// "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx = "message\":\"\\[(.*?)\\] DEBUG";
		String regx2 = "iataCode(.*?)storedRHCValue";
		String regx3 = "Balance Alerts Incoming Payload: (.*?)\",\"path";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;
		List<String> date = new ArrayList<String>();
		List<String> iataCode = new ArrayList<String>();
		List<String> payload = new ArrayList<String>();

		// 定义文件名格式并创建

		File file1 = new File("c://Users/wangjm@iata.org/Desktop/" + "date.txt");
		file1.createNewFile();
		File file2 = new File("c://Users/wangjm@iata.org/Desktop/" + "iataCode.txt");
		file2.createNewFile();
		File file3 = new File("c://Users/wangjm@iata.org/Desktop/" + "payload.txt");
		file3.createNewFile();

		FileWriter fw1 = new FileWriter(file1);
		FileWriter fw2 = new FileWriter(file2);
		FileWriter fw3 = new FileWriter(file3);

		// get 404 failed ids
		for (int i = 0; i <= pageNumUpdate; i++) {

			// System.out.println("------------------Page: " + i + " Reason 404
			// ---------------------");

//			for (SearchHit hit : hits4BalanceUpdate.getHits()) {
//				System.out.println(hit.getSourceAsString());
//				match = pattern.matcher(hit.getSourceAsString());
//				if (match.find()) {
//					date.add(match.group(1));
//					// fw.write(match.group(1));
//					// fw.write("\r\n");
//				} else {
//					date.add("");
//				}
//				System.out.println(match.group(1));
//
//				match = pattern2.matcher(hit.getSourceAsString());
//				if (match.find()) {
//					iataCode.add(match.group(1));
//					// System.out.println(match.group(1));
//				} else {
//					iataCode.add("");
//				}
//
//				match = pattern3.matcher(hit.getSourceAsString());
//				if (match.find()) {
//					payload.add(match.group(1));
//					// System.out.println(match.group(1));
//				} else {
//					payload.add("");
//				}
//
//				// System.out.println("**************************************************************************************");
//			}
			


				// System.out.println("------------------Page: " + i + " Reason Unknown
				// ---------------------");
				for (SearchHit hit : response4BalanceUpdate.getHits()) {
//					System.out.println(hit.getSourceAsString());
					match = pattern.matcher(hit.getSourceAsString());
					if (match.find()) {
						date.add(match.group(1));
						fw1.write(match.group(1));
						fw1.write("\r\n");
					}
//					System.out.println(match.group(1));
	
					match = pattern2.matcher(hit.getSourceAsString());
					if (match.find()) {
						iataCode.add(match.group(1));
						fw2.write(match.group(1));
						fw2.write("\r\n");
					}
//					 System.out.println(match.group(1));
	
					match = pattern3.matcher(hit.getSourceAsString());
					if (match.find()) {
						payload.add(match.group(1));
						fw3.write(match.group(1));
						fw3.write("\r\n");
					} 
					 System.out.println(match.group(1));
				}

				response4BalanceUpdate = es.client.prepareSearchScroll(response4BalanceUpdate.getScrollId())
						.setScroll(new TimeValue(20000)).get();
			

		}
		fw1.close();
		fw2.close();
		fw3.close();

		System.out.println("Balance Update Hits Count: " + hits4BalanceUpdate.totalHits);

		es.close();

	}
}
