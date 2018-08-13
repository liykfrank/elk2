package elasticsearch;

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

public class GetAgentCode {
	
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
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName(AgencyUpdate.HOST), AgencyUpdate.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}
	
	
	public static void main(String[] args) throws IOException{
		
		GetAgentCode es = new GetAgentCode();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(AgencyUpdate.START).to(AgencyUpdate.END);
		
		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/AgencyXml.txt"));
		
		String regx = "Client>(.*?)</a:Client";
		Pattern pattern = Pattern.compile(regx);
		Matcher match = null;
		
		List<String> listTemp = new ArrayList<String>();
		System.out.println("****************");
		for (String code : lines) {
//			System.out.println(code);
			match = pattern.matcher(code);
			if(match.find())
				listTemp.add(match.group(1));
		}
		
		for (String account : listTemp) {
			System.out.println(account);
			
//			QueryBuilder qbINCid = QueryBuilders.boolQuery().must(qbTime)
//					.must(QueryBuilders.matchPhraseQuery("message", account))
//					.must(QueryBuilders.matchPhraseQuery("message", "BalanceUpdate"))
//					.must(QueryBuilders.matchPhraseQuery("message", "Incoming Payload"));
//			
//			SearchResponse response4INC = es.client.prepareSearch("newgen_balance-alerts-service").setTypes("log")
//					.setQuery(qbINCid).addSort("@timestamp", SortOrder.DESC).get();
//			
//			SearchHits hits4INC = response4INC.getHits();
//			
//			for (SearchHit hit : hits4INC) {
//				System.out.println(hit.getSourceAsString());
//			}
			
		}
		
		
		es.close();
		
	}

}
