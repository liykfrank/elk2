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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class BalanceAlertByIataCode {


	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-07-30T15:59:59.000Z";
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
				new TransportAddress(InetAddress.getByName(BalanceAlertByIataCode.HOST), BalanceAlertByIataCode.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		BalanceAlertByIataCode es = new BalanceAlertByIataCode();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(BalanceAlertByIataCode.START).to(BalanceAlertByIataCode.END);

		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/iatacode.txt"));
		
		System.out.println("*********************************");
		
		// remove duplicated
		List<String> listTemp = new ArrayList<String>();
		for (String code : lines) {
			if(!listTemp.contains(code))
				listTemp.add(code);
		}
		
		System.out.println("*********************************");

		File file = new File("c:/Users/wangjm@iata.org/Desktop/result.csv");
		FileWriter fw = new FileWriter(file);
		
		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "@timestamp\":\"(.*?)\"";
		String regx3 = "Incoming Payload:(.*?)}";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;

		for (String INCid : listTemp) {
			QueryBuilder qbINCid = QueryBuilders.boolQuery().must(qbTime)
					.must(QueryBuilders.matchPhraseQuery("message", INCid))
					.must(QueryBuilders.matchPhraseQuery("message", "BalanceUpdate"))
					.must(QueryBuilders.matchPhraseQuery("message", "Incoming Payload"));

			SearchResponse response4INC = es.client.prepareSearch("newgen_balance-alerts-service").setTypes("log")
					.setQuery(qbINCid).addSort("@timestamp", SortOrder.DESC).get();

			SearchHits hits4INC = response4INC.getHits();

			System.out.println(INCid);

			if (hits4INC.totalHits == 0) {
				fw.write(INCid);
				fw.write(",");
				fw.write("None");
				fw.write("\r\n");
			} else {
				for (SearchHit hit : hits4INC) {
					fw.write(INCid);
					fw.write(",");
					System.out.println(hit.getSourceAsString());
					match = pattern2.matcher(hit.getSourceAsString());
					if (match.find()) {
						fw.write(match.group(1));
						fw.write(",");
					}
					match = pattern3.matcher(hit.getSourceAsString());
					if (match.find()) {
						String str = match.group(1);
						String payload = str.replaceAll(",", " ");
						fw.write(payload);
					}
					fw.write("\r\n");
				}
			}
				
			

		}

		fw.close();
		es.close();
	}





}
