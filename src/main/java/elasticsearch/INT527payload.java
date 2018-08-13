package elasticsearch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
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

public class INT527payload {

	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-05-01T15:59:59.000Z";
	public final static String END = "2018-06-01T15:59:59.000Z";

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
				new TransportAddress(InetAddress.getByName(INT527payload.HOST), INT527payload.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		INT527payload es = new INT527payload();
		es.init();

		// CEST +02:00 中欧夏令时
		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT527payload.START).to(INT527payload.END);

		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/timeout.txt"));

		File file = new File("c:/Users/wangjm@iata.org/Desktop/timeoutList.txt");
		FileWriter fw = new FileWriter(file);

		for (String INCid : lines) {
			QueryBuilder qbINCid = QueryBuilders.boolQuery().must(qbTime)
					.must(QueryBuilders.matchPhraseQuery("message", INCid));

			SearchResponse response4INC = es.client.prepareSearch("newgen_servicenow-system-service").setTypes("log")
					.setQuery(qbINCid).addSort("@timestamp", SortOrder.DESC).get();

			SearchHits hits4INC = response4INC.getHits();

			String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
			String regx2 = "content(.*?)content-type";
			String regx3 = "phone\":\"(.*?)\"";
			Pattern pattern = Pattern.compile(regx);
			Pattern pattern2 = Pattern.compile(regx2);
			Pattern pattern3 = Pattern.compile(regx3);
			Matcher match = null;

			for (SearchHit hit : hits4INC) {
				match = pattern.matcher(hit.getSourceAsString());
				if (match.find()) {
					// Query log by ID and is not known reason
					// System.out.println(match.group(1));
					QueryBuilder qbID = QueryBuilders.boolQuery()
							.must(QueryBuilders.matchPhraseQuery("message", match.group(1))).must(QueryBuilders
									.matchPhraseQuery("message", "ServiceNow Request Received with payload:"))
							.must(qbTime);

					SearchResponse response4RMEid = es.client.prepareSearch("newgen*").setTypes("log").setQuery(qbID)
							.get();

					SearchHits hits4RMEid = response4RMEid.getHits();

					for (SearchHit hitRME : hits4RMEid) {

						System.out.println(hitRME.getSourceAsString());
						match = pattern2.matcher(hitRME.getSourceAsString());

						fw.write(INCid);
						fw.write("  ");
						if (match.find()) {

							String payload = match.group(1);
							String payloadd = payload.substring(6, payload.length() - 13);
							// System.out.println(payload);
							System.out.println(payloadd);

							byte[] decodedBytes = Base64.getDecoder().decode(payloadd);
							String decodedPayload = new String(decodedBytes);

							System.out.println(decodedPayload);

							match = pattern3.matcher(decodedPayload);

							if (match.find()) {

								String phone = match.group(1);
								System.out.println(phone);

								QueryBuilder qbRpayload = QueryBuilders.boolQuery()
										.must(QueryBuilders.matchPhraseQuery("message", phone))
										.must(QueryBuilders.matchPhraseQuery("message", "Timeout exceeded"))
										.must(qbTime);

								SearchResponse responseRpayload = es.client.prepareSearch("newgen*").setTypes("log")
										.setQuery(qbRpayload).addSort("@timestamp", SortOrder.DESC).get();

								SearchHits hits4Rpayload = responseRpayload.getHits();

								System.out.println(hits4Rpayload.totalHits);

								if (hits4Rpayload.totalHits > 0) {
									fw.write(phone);
									fw.write("  ");
									fw.write("YES");
								} else {
									fw.write("null");
									fw.write("  ");
									fw.write("NO");
								}
							} else {
								fw.write("null");
								fw.write("  ");
								fw.write("NO");
							}

						} else {
							fw.write("null");
							fw.write("  ");
							fw.write("NO");
						}
						fw.write("\r\n");
					}
					System.out.println(
							"---------------------------------Payload--------------------------------------------");
				}

			}

		}

		fw.close();
		es.close();
	}

}
