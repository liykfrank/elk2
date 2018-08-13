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

public class INT527 {

	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-05-01T15:59:59.000Z";
	public final static String END = "2018-05-29T15:59:59.000Z";

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
				.addTransportAddress(new TransportAddress(InetAddress.getByName(INT527.HOST), INT527.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		INT527 es = new INT527();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT527.START).to(INT527.END);

		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/int528.txt"));
		for (String INCid : lines) {
			System.out.println(INCid);
			QueryBuilder qbINCid = QueryBuilders.boolQuery().must(qbTime)
					.must(QueryBuilders.matchPhraseQuery("message", INCid));

			SearchResponse response4INC = es.client.prepareSearch("newgen_servicenow-system-service").setTypes("log")
					.setQuery(qbINCid).addSort("@timestamp", SortOrder.DESC).get();

			SearchHits hits4INC = response4INC.getHits();
//			System.out.println(hits4INC.totalHits);

			String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
			Pattern pattern = Pattern.compile(regx);
			Matcher match = null;

			for (SearchHit hit : hits4INC) {
				match = pattern.matcher(hit.getSourceAsString());
				if (match.find()) {
					// Query log by ID and is not known reason
					QueryBuilder qbID = QueryBuilders.boolQuery()
							.must(QueryBuilders.matchPhraseQuery("message", match.group(1))).must(qbTime);

					SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_featurespace-system-service")
							.setTypes("log").setQuery(qbID).get();

					SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
//					System.out.println(hits4IDUnknown.totalHits);

					for (SearchHit hit2 : hits4IDUnknown) {
						match = pattern.matcher(hit2.getSourceAsString());
						if (match.find()) {
							// Query log by ID and is not known reason
							QueryBuilder qbID2 = QueryBuilders.boolQuery()
									.must(QueryBuilders.matchPhraseQuery("message", match.group(1))).must(qbTime);

							SearchResponse responseRME = es.client.prepareSearch("newgen_featurespace-system-service")
									.setTypes("log")
									.setQuery(qbID2)
									.addSort("@timestamp", SortOrder.DESC)
									.get();

							SearchHits hits4IDRME = responseRME.getHits();

							File file = null;

							File fileRME = null;

							FileWriter fw = null;
//							String dirName = "INT527 " + sdf.format(now) + " " + now.getTime();
							String dirName = "snow";
							String dirNameRME = "RME";
							String path = "c:/Users/wangjm@iata.org/Desktop/";

							file = new File(path + dirName);
							fileRME = new File(path + dirName + "/" + dirNameRME);

							file.mkdirs();
							fileRME.mkdirs();

							file = new File(path + dirName + "/" + dirNameRME + "/", INCid + ".txt");
							try {
								fw = new FileWriter(file);
								for (SearchHit hit3 : hits4IDRME) {
									System.out.println(hit3.getSourceAsString());
									fw.write(hit3.getSourceAsString());
									fw.write("\r\n");
								}

							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} finally {
								fw.close();
							}

							System.out.println(
									"---------------------------------RME Failed--------------------------------------------");
						}
					}
				}
			}


		}

		es.close();
	}
}
