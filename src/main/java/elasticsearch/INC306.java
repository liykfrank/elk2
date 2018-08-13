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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class INC306 {


	public final static String HOST = "10.140.8.212";
	public final static int PORT = 9300;
	public final static String START = "2018-05-01T15:59:59.000Z";
	public final static String END = "2018-06-29T15:59:59.000Z";

	TransportClient client;

	private void init() throws UnknownHostException {
		// on startup
		// TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
		// Settings settings = Settings.builder().put("cluster.name", "name").build();
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(InetAddress.getByName(INC306.HOST), INC306.PORT));
		System.out.println("Elasticsearch connect info: " + client.nodeName());
	}

	private void close() {
		// on shutdown
		client.close();
	}

	public static void main(String[] args) throws IOException {

		INC306 es = new INC306();
		es.init();

		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INC306.START).to(INC306.END);

		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		String regx2 = "Transaction ID: (.*?)\\).";
		String regx3 = "@timestamp\":\"(.*?)\"";
		Pattern pattern = Pattern.compile(regx);
		Pattern pattern2 = Pattern.compile(regx2);
		Pattern pattern3 = Pattern.compile(regx3);
		Matcher match = null;

		List<String> notMatch = new ArrayList<String>();
		List<String> lines = Files.readAllLines(Paths.get("c://Users/wangjm@iata.org/Desktop/snow.txt"));
		for (String INCid : lines) {

			QueryBuilder qbINCid = QueryBuilders.boolQuery().must(qbTime)
					.must(QueryBuilders.matchPhraseQuery("message", INCid));

			SearchResponse response4INC = es.client.prepareSearch("newgen_servicenow-system-service").setTypes("log")
					.setQuery(qbINCid).addSort("@timestamp", SortOrder.DESC).get();

			SearchHits hits4INC = response4INC.getHits();
			// System.out.println(hits4INC.totalHits);

			for (SearchHit hit : hits4INC) {
				match = pattern.matcher(hit.getSourceAsString());
				if (match.find()) {
					String tID = match.group(1);
					System.out.println(tID);

					// ********************************************************
					// case 1: search by Transaction ID and incident rang time
					//
					QueryBuilder qbID = QueryBuilders
							.boolQuery().must(QueryBuilders.matchPhraseQuery("message", tID)).must(QueryBuilders
									.matchPhraseQuery("message", "ServiceNow Request Received with payload:"))
							.must(qbTime);

					// get Transaction ID
					SearchResponse responseTid = es.client.prepareSearch("newgen_servicenow-system-service")
							.setTypes("log").setQuery(qbID).get();

					SearchHits hits4ID = responseTid.getHits();

					for (SearchHit hit2 : hits4ID) {
						match = pattern2.matcher(hit2.getSourceAsString());

						if (match.find()) {
							String tID2 = match.group(1);
							System.out.println(tID2);
							// get Transaction ID
							QueryBuilder qbID2 = QueryBuilders.boolQuery()
									.must(QueryBuilders.matchPhraseQuery("message", tID2)).must(qbTime);

							SearchResponse responseRME = es.client.prepareSearch("newgen_featurespace-system-service")
									.setTypes("log").setQuery(qbID2).addSort("@timestamp", SortOrder.ASC).get();

							SearchHits hits4IDRME = responseRME.getHits();

							for (SearchHit hitt : hits4IDRME) {
								System.out.println(hitt.getSourceAsString());
							}

							System.out.println("--------------------------------------");
							if (hits4IDRME.totalHits > 0) {
								String first = hits4IDRME.getAt(0).getSourceAsString();
								String last = null;
								for (SearchHit hitt : hits4IDRME) {
									last = hitt.getSourceAsString();
								}
								// String last = hits4IDRME.getAt(9).getSourceAsString();

								System.out.println(first);
								System.out.println(last);

								match = pattern3.matcher(first);
								String start = null;
								String end = null;
								if (match.find())
									start = match.group(1);
								match = pattern3.matcher(last);
								if (match.find())
									end = match.group(1);

								System.out.println(start);
								System.out.println(end);

								QueryBuilder time = QueryBuilders.rangeQuery("@timestamp").from(start).to(end);

								SearchResponse response = es.client.prepareSearch("newgen_featurespace-system-service")
										.setTypes("log").setQuery(time).setScroll(TimeValue.timeValueMinutes(7))
										.setSize(10).addSort("@timestamp", SortOrder.ASC).get();

								SearchHits hits = response.getHits();

								// shades * size
								int pageNum = (int) hits.totalHits / (1 * 10);

								File file = null;

								File fileRME = null;

								FileWriter fw = null;
								// String dirName = "INC306 " + sdf.format(now) + " " + now.getTime();
								String dirName = "snow";
								String dirNameRME = "531";
								String path = "c:/Users/wangjm@iata.org/Desktop/";

								file = new File(path + dirName);
								fileRME = new File(path + dirName + "/" + dirNameRME);

								file.mkdirs();
								fileRME.mkdirs();

								file = new File(path + dirName + "/" + dirNameRME + "/", INCid + ".txt");
								try {
									fw = new FileWriter(file);
									for (int i = 0; i <= pageNum; i++) {

										// System.out.println("------------------Page: " + i + " Reason Unknown
										// ---------------------");
										for (SearchHit hitt : response.getHits()) {
											System.out.println(hitt.getSourceAsString());
											fw.write(hitt.getSourceAsString());
											fw.write("\r\n");
										}

										response = es.client.prepareSearchScroll(response.getScrollId())
												.setScroll(new TimeValue(20000)).get();
									}

								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} finally {
									fw.close();
								}

							} else {
								notMatch.add(INCid);
							}

						}

						System.out.println(
								"---------------------------------RME Failed--------------------------------------------");
					}
				}
			}
		}

		es.close();

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Not Match case1 List: ");
		for (String line : notMatch)
			System.out.println(line);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	}



}
