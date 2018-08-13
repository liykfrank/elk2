package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
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

public class AgencyUpdate {


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

	public static void main(String[] args) throws IOException, ParseException {

		AgencyUpdate es = new AgencyUpdate();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(AgencyUpdate.START).to(AgencyUpdate.END);

		File file = new File("c:/Users/wangjm@iata.org/Desktop/AgencyUpdate.csv");
		FileWriter fw = new FileWriter(file);
		fw.write("notID");
		fw.write(",");
		fw.write("Time");
		fw.write(",");
		fw.write("IATA_Code");
		fw.write(",");
		fw.write("Country");
		fw.write(",");
		fw.write("isNewGen");
		fw.write("\r\n");

		QueryBuilder qb = QueryBuilders.boolQuery().must(qbTime)
				.must(QueryBuilders.matchPhraseQuery("message", "Agency Update request received for Agency"));

		SearchResponse response4INC = es.client.prepareSearch("newgen_iata-agencies*").setTypes("log").setQuery(qb)
				.setSearchType(SearchType.DEFAULT).setScroll(TimeValue.timeValueMinutes(7)).setSize(10)
				.addSort("@timestamp", SortOrder.DESC).get();

		SearchHits hits4INC = response4INC.getHits();
		int pageNumUpdate1 = (int) hits4INC.totalHits / (1 * 10);

		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "Agency: (.*?)with payload";
		String regx3 = "billingCountry(.*?)billingCity";
		String regx4 = "@timestamp\":\"(.*?)\"";
		String regx5 = "isNewGenAgency(.*?)IATACode";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Pattern pattern4 = Pattern.compile(regx4);
		Pattern pattern5 = Pattern.compile(regx5);
		Matcher match = null;
		

		for (int i = 0; i <= pageNumUpdate1; i++) {
			for (SearchHit hit : response4INC.getHits()) {

				System.out.println(hit.getSourceAsString());

				// match id
				match = pattern.matcher(hit.getSourceAsString());
				if (match.find()) {
					System.out.println(match.group(1));
					fw.write(match.group(1));
					fw.write(",");
					
					QueryBuilder qbID = QueryBuilders.boolQuery()
							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
							.must(QueryBuilders.matchPhraseQuery("message", "Agency Response from SFDC System API")).must(qbTime);
					SearchResponse response4RMEid = es.client.prepareSearch("newgen_*").setTypes("log").setQuery(qbID)
							.get();
					SearchHits hits4RMEid = response4RMEid.getHits();
					
					for (SearchHit hitRME : hits4RMEid) {
						
						// match time
						match = pattern4.matcher(hit.getSourceAsString());
						if (match.find()) {
							System.out.println(match.group(1));
							fw.write(match.group(1));
							fw.write(",");
						} else {
							fw.write("Not match time");
							fw.write(",");
						}
						
						// match code
						match = pattern2.matcher(hit.getSourceAsString());
						if (match.find()) {
							System.out.println(match.group(1));
							fw.write(match.group(1));
							fw.write(",");
						} else {
							fw.write("Not match iata code");
							fw.write(",");
						}
						
						System.out.println(hitRME.getSourceAsString());
						
						// match country
						match = pattern3.matcher(hitRME.getSourceAsString());
						if (match.find()) {
							String bCountry = match.group(1);
							String country = bCountry.substring(5, bCountry.length() - 5);
							String country2 = country.replace(",", " ");
							System.out.println(country2);
							fw.write(country2);
							fw.write(",");
						} else {
							fw.write("Not match country");
							fw.write(",");
						}
						
						// match NewGenAgency
						match = pattern5.matcher(hitRME.getSourceAsString());
						if (match.find()) {
							String str = match.group(1);
							String isNewGenAgency = str.substring(3, str.length() - 3);
							System.out.println(isNewGenAgency);
							fw.write(isNewGenAgency);
							fw.write("\r\n");
						} else {
							fw.write("Not match isNewGenAgency");
							fw.write("\r\n");
						}
					}

					if (hits4RMEid.totalHits < 1) {
						fw.write("No Response from RME");
						fw.write("\r\n");
					}
				}

				System.out.println(
						"---------------------------------Payload--------------------------------------------");
			}

			response4INC = es.client.prepareSearchScroll(response4INC.getScrollId()).setScroll(new TimeValue(20000))
					.get();
		}

		fw.close();
		es.close();
	}


}
