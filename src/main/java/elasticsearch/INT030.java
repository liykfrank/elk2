//package elasticsearch;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.TransportAddress;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.sort.SortOrder;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//
//public class INT030 {
//
//	public final static String HOST = "10.140.8.212";
//	public final static int PORT = 9300;
//<<<<<<< Updated upstream
//	public final static String START = "2018-06-06T15:59:59.000Z";
//	public final static String END = "2018-06-08T15:59:59.000Z";
//	private static final String reportPath = "./src/main/resources/INT030_Report.csv";
//	private static String path = "./src/main/resources/";
//=======
//	public final static String START = "2018-06-08T15:59:59.000Z";
//	public final static String END = "2018-06-11T15:59:59.000Z";
//>>>>>>> Stashed changes
//
//	TransportClient client;
//
//	private void init() throws UnknownHostException {
//		// on startup
//		// TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
//		// Settings settings = Settings.builder().put("cluster.name", "name").build();
//		client = new PreBuiltTransportClient(Settings.EMPTY)
//				.addTransportAddress(new TransportAddress(InetAddress.getByName(INT030.HOST), INT030.PORT));
//		System.out.println("Elasticsearch connect info: " + client.nodeName());
//	}
//
//	private void close() {
//		// on shutdown
//		client.close();
//	}
//
//	public static void main(String[] args) throws IOException {
//
//		INT030 es = new INT030();
//		es.init();
//
//		// CEST +02:00 中欧夏令时
//		QueryBuilder qbTime = QueryBuilders.rangeQuery("@timestamp").from(INT030.START).to(INT030.END);
//
//		// RS-Balances request Payload
//
//		// RME : routing to Featurespace (RME)
//		// ENDRED : RS-Balances routing to matched IEP Vendor - EDENRED
//		// MSTS : RS-Balances routing to matched IEP Vendor - MSTS
//
//		// RME : Featurespace SystemAPI Response & "code": "200"
//		// EMDRED : RS-Balances Response Received from EDENRED & "code": "202"
//		// MSTS : RS-Balances Response Received from MSTS & "code": "202"
//
//		QueryBuilder qbRSBalance4RMEReceieved = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "routing to Featurespace (RME)"));
//
//		QueryBuilder qbRSBalance4EDENREDReceieved = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances routing to matched IEP Vendor - EDENRED"));
//
//		QueryBuilder qbRSBalance4MSTSReceieved = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances routing to matched IEP Vendor - MSTS"));
//
//		QueryBuilder qbRSBalance4RMESuccess = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "Featurespace SystemAPI Response"))
//				.must(QueryBuilders.matchPhraseQuery("message", "\"code\": \"200\""));
//
//		QueryBuilder qbRSBalance4EDENREDSuccess = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances Response Received from EDENRED"))
//				.must(QueryBuilders.matchPhraseQuery("message", "\"code\": \"202\""));
//
//		QueryBuilder qbRSBalance4MSTSSuccess = QueryBuilders.boolQuery().must(qbTime)
//				.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances Response Received from MSTS"))
//				.must(QueryBuilders.matchPhraseQuery("message", "\"code\": \"202\""));
//
//		SearchResponse response4RSBalanceRME = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//				.setQuery(qbRSBalance4RMEReceieved).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchResponse response4RSBalanceEDENRED = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//				.setQuery(qbRSBalance4EDENREDReceieved).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchResponse response4RSBalanceMSTS = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//				.setQuery(qbRSBalance4MSTSReceieved).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchResponse response4RSBalanceRMESuccess = es.client.prepareSearch("newgen_rs-balances-service")
//				.setTypes("log").setQuery(qbRSBalance4RMESuccess).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchResponse response4RSBalanceEDENREDSuccess = es.client.prepareSearch("newgen_rs-balances-service")
//				.setTypes("log").setQuery(qbRSBalance4EDENREDSuccess).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchResponse response4RSBalanceMSTSSuccess = es.client.prepareSearch("newgen_rs-balances-service")
//				.setTypes("log").setQuery(qbRSBalance4MSTSSuccess).setSearchType(SearchType.DEFAULT)
//				.setScroll(TimeValue.timeValueMinutes(7)).setSize(10).addSort("@timestamp", SortOrder.DESC).get();
//
//		SearchHits hits4RME = response4RSBalanceRME.getHits();
//		SearchHits hits4EDENRED = response4RSBalanceEDENRED.getHits();
//		SearchHits hits4MSTS = response4RSBalanceMSTS.getHits();
//		SearchHits hits4RMESuccess = response4RSBalanceRMESuccess.getHits();
//		SearchHits hits4EDENREDSuccess = response4RSBalanceEDENREDSuccess.getHits();
//		SearchHits hits4MSTSSuccess = response4RSBalanceMSTSSuccess.getHits();
//
//		// shades * size
//		int pageNumRME = (int) hits4RME.totalHits / (1 * 10);
//		int pageNumEDENRED = (int) hits4EDENRED.totalHits / (1 * 10);
//		int pageNumMSTS = (int) hits4MSTS.totalHits / (1 * 10);
//
//		// Get IDs from each hit
//		String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
//		// String regx4 = "<Operation>.*{3}(.*?).*{5}</Operation>";
//		String regx2 = "<Operation>(.*?)</Operation>";
//		String regx3 = "<a:Client>(.*?)</a:Client>";
//		Pattern pattern = Pattern.compile(regx);
//		Pattern pattern2 = Pattern.compile(regx2);
//		Pattern pattern3 = Pattern.compile(regx3);
//		// Pattern pattern4 = Pattern.compile(regx4);
//		Matcher match = null;
//		List<String> idsRME = new ArrayList<String>();
//		List<String> idsEDENRED = new ArrayList<String>();
//		List<String> idsMSTS = new ArrayList<String>();
//
//		File report = new File(reportPath);
//		FileWriter fwr = new FileWriter(report);
//
//		// Title
//		fwr.write("ID,Operation,BSP/CAS/IEP,Code1,Code2,Code3,Status");
//		fwr.write("\r\n");
//
//		// get unknown failed idsRME
//		long startTime = System.currentTimeMillis();
//		for (int i = 0; i <= pageNumRME; i++) {
//
//			// System.out.println("------------------Page: " + i + " Reason RME
//			// ---------------------");
//			for (SearchHit hit : response4RSBalanceRME.getHits()) {
//				match = pattern.matcher(hit.getSourceAsString());
//				// query log by ID
//				if (match.find()) {
//					// Query log by ID and is not known reason
//
//					fwr.write(match.group(1));
//					fwr.write(",");
//
//					QueryBuilder qb1 = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances request Payload:"))
//							.must(qbTime);
//
//					SearchResponse response1 = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//							.setQuery(qb1).get();
//
//					SearchHits hits1 = response1.getHits();
//
//					if (hits1.totalHits > 0) {
//						for (SearchHit hit1 : response1.getHits()) {
//							match = pattern2.matcher(hit1.getSourceAsString());
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//								String bsp = match.group(1).substring(3, 6);
//								// System.out.println(bsp);
//								fwr.write(bsp);
//								fwr.write(",");
//							} else {
//								fwr.write("not match operation");
//								fwr.write(",");
//								fwr.write("not match operation");
//								fwr.write(",");
//							}
//
//							match = pattern3.matcher(hit1.getSourceAsString());
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("one");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("two");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("three");
//								fwr.write(",");
//							}
//
//						}
//
//					} else {
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//					}
//
//					match = pattern.matcher(hit.getSourceAsString());
//					match.find();
//
//					QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(qbRSBalance4RMESuccess).must(qbTime);
//
//					SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_rs-balances-service")
//							.setTypes("log").setQuery(qbIDUnknow).get();
//
//					SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
//					// if don't match known
//					if (hits4IDUnknown.getTotalHits() == 0) {
//						idsRME.add(match.group(1));
//						fwr.write("Failed");
//						fwr.write("\r\n");
//					} else {
//						fwr.write("Success");
//						fwr.write("\r\n");
//					}
//				}
//			}
//			response4RSBalanceRME = es.client.prepareSearchScroll(response4RSBalanceRME.getScrollId())
//					.setScroll(new TimeValue(20000)).get();
//		}
//		long endTime = System.currentTimeMillis();
//
//		// get unknown failed idsEDENRED
//		for (int i = 0; i <= pageNumEDENRED; i++) {
//
//			// System.out.println("------------------Page: " + i + " Reason EDENRED
//			// ---------------------");
//			for (SearchHit hit : response4RSBalanceEDENRED.getHits()) {
//				match = pattern.matcher(hit.getSourceAsString());
//				// query log by ID
//				if (match.find()) {
//					// Query log by ID and is not known reason
//
//					fwr.write(match.group(1));
//					fwr.write(",");
//
//					QueryBuilder qb1 = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances request Payload:"))
//							.must(qbTime);
//
//					SearchResponse response1 = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//							.setQuery(qb1).get();
//
//					SearchHits hits1 = response1.getHits();
//
//					if (hits1.totalHits > 0) {
//						for (SearchHit hit1 : response1.getHits()) {
//							match = pattern2.matcher(hit1.getSourceAsString());
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//								String bsp = match.group(1).substring(3, 6);
//								// System.out.println(bsp);
//								fwr.write(bsp);
//								fwr.write(",");
//							} else {
//								fwr.write("not match operation");
//								fwr.write(",");
//								fwr.write("not match operation");
//								fwr.write(",");
//							}
//
//							match = pattern3.matcher(hit1.getSourceAsString());
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("one");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("two");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("three");
//								fwr.write(",");
//							}
//
//						}
//					} else {
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//					}
//
//					match = pattern.matcher(hit.getSourceAsString());
//					match.find();
//
//					QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(qbRSBalance4EDENREDSuccess).must(qbTime);
//
//					SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_rs-balances-service")
//							.setTypes("log").setQuery(qbIDUnknow).get();
//
//					SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
//					// if don't match known
//					if (hits4IDUnknown.getTotalHits() == 0) {
//						idsEDENRED.add(match.group(1));
//						fwr.write("Failed");
//						fwr.write("\r\n");
//					} else {
//						fwr.write("Success");
//						fwr.write("\r\n");
//					}
//
//				}
//			}
//			response4RSBalanceEDENRED = es.client.prepareSearchScroll(response4RSBalanceEDENRED.getScrollId())
//					.setScroll(new TimeValue(20000)).get();
//		}
//
//		// get unknown failed idsMSTS
//		for (int i = 0; i <= pageNumMSTS; i++) {
//
//			// System.out.println("------------------Page: " + i + " Reason MSTS
//			// ---------------------");
//			for (SearchHit hit : response4RSBalanceMSTS.getHits()) {
//				match = pattern.matcher(hit.getSourceAsString());
//				// query log by ID
//				if (match.find()) {
//					// Query log by ID and is not known reason
//
//					fwr.write(match.group(1));
//					fwr.write(",");
//
//					QueryBuilder qb1 = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(QueryBuilders.matchPhraseQuery("message", "RS-Balances request Payload:"))
//							.must(qbTime);
//
//					SearchResponse response1 = es.client.prepareSearch("newgen_rs-balances-service").setTypes("log")
//							.setQuery(qb1).get();
//
//					SearchHits hits1 = response1.getHits();
//
//					match = pattern.matcher(hit.getSourceAsString());
//					match.find();
//
//					if (hits1.totalHits > 0) {
//						for (SearchHit hit1 : response1.getHits()) {
//							match = pattern2.matcher(hit1.getSourceAsString());
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//								String bsp = match.group(1).substring(3, 6);
//								// System.out.println(bsp);
//								fwr.write(bsp);
//								fwr.write(",");
//							} else {
//								fwr.write("not match operation");
//								fwr.write(",");
//								fwr.write("not match operation");
//								fwr.write(",");
//							}
//
//							match = pattern3.matcher(hit1.getSourceAsString());
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("one");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("two");
//								fwr.write(",");
//							}
//
//							if (match.find()) {
//								fwr.write(match.group(1));
//								fwr.write(",");
//								// System.out.println(match.group(1));
//							} else {
//								fwr.write("three");
//								fwr.write(",");
//							}
//
//						}
//					} else {
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//						fwr.write("no payload");
//						fwr.write(",");
//					}
//
//					QueryBuilder qbIDUnknow = QueryBuilders.boolQuery()
//							.must(QueryBuilders.matchPhraseQuery("message", match.group(1)))
//							.must(qbRSBalance4MSTSSuccess).must(qbTime);
//
//					SearchResponse responseUpadateKnown = es.client.prepareSearch("newgen_rs-balances-service")
//							.setTypes("log").setQuery(qbIDUnknow).get();
//
//					SearchHits hits4IDUnknown = responseUpadateKnown.getHits();
//					// if don't match known
//					if (hits4IDUnknown.getTotalHits() == 0) {
//						idsMSTS.add(match.group(1));
//						fwr.write("Failed");
//						fwr.write("\r\n");
//					} else {
//						fwr.write("Success");
//						fwr.write("\r\n");
//					}
//				}
//			}
//			response4RSBalanceMSTS = es.client.prepareSearchScroll(response4RSBalanceMSTS.getScrollId())
//					.setScroll(new TimeValue(20000)).get();
//		}
//
//		File file = null;
//
//		File fileRME = null;
//		File fileEDENRED = null;
//		File fileMSTS = null;
//
//		FileWriter fw = null;
//		Date now = new Date();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		String dirName = "INT030 " + sdf.format(now) + " " + now.getTime();
//		String dirNameRME = "RME";
//		String dirNameEDENRED = "EDENRED";
//		String dirNameMSTS = "MSTS";
//		
//
//		file = new File(path + dirName);
//		fileRME = new File(path + dirName + "/" + dirNameRME);
//		fileEDENRED = new File(path + dirName + "/" + dirNameEDENRED);
//		fileMSTS = new File(path + dirName + "/" + dirNameMSTS);
//
//		file.mkdirs();
//		fileRME.mkdirs();
//		fileEDENRED.mkdirs();
//		fileMSTS.mkdirs();
//
//		// Query and Write RME Failed
//		for (String id : idsRME) {
//			QueryBuilder qbIDUnknowFailed = QueryBuilders.boolQuery()
//					.must(QueryBuilders.matchPhraseQuery("message", id)).must(qbTime);
//			// 对应的ID 日志不会很多 不用分页
//			SearchResponse response4RMEFailed = es.client.prepareSearch("newgen*").setTypes("log")
//					.setQuery(qbIDUnknowFailed).setSize(100).addSort("@timestamp", SortOrder.DESC).get();
//
//			SearchHits hits4RMEFailed = response4RMEFailed.getHits();
//
//			file = new File(path + dirName + "/" + dirNameRME + "/", id + ".txt");
//			try {
//				fw = new FileWriter(file);
//				for (SearchHit hit : hits4RMEFailed) {
//					System.out.println(hit.getSourceAsString());
//					fw.write(hit.getSourceAsString());
//					fw.write("\r\n");
//				}
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//				fw.close();
//			}
//
//			System.out
//					.println("---------------------------------RME Failed--------------------------------------------");
//		}
//
//		// Query and Write EDENRED Failed
//		for (String id : idsEDENRED) {
//			QueryBuilder qbIDEDENREDFailed = QueryBuilders.boolQuery()
//					.must(QueryBuilders.matchPhraseQuery("message", id)).must(qbTime);
//			// 对应的ID 日志不会很多 不用分页
//			SearchResponse response4EDENREDFailed = es.client.prepareSearch("newgen*").setTypes("log")
//					.setQuery(qbIDEDENREDFailed).setSize(100).addSort("@timestamp", SortOrder.DESC).get();
//
//			SearchHits hits4EDENREDFailed = response4EDENREDFailed.getHits();
//
//			file = new File(path + dirName + "/" + dirNameEDENRED + "/", id + ".txt");
//			try {
//				fw = new FileWriter(file);
//				for (SearchHit hit : hits4EDENREDFailed) {
//					System.out.println(hit.getSourceAsString());
//					fw.write(hit.getSourceAsString());
//					fw.write("\r\n");
//				}
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//				fw.close();
//			}
//
//			System.out.println(
//					"---------------------------------EDENRED Failed--------------------------------------------");
//		}
//
//		// Query and Write MSTS Failed
//		for (String id : idsMSTS) {
//			QueryBuilder qbIDMSTSFailed = QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("message", id))
//					.must(qbTime);
//			// 对应的ID 日志不会很多 不用分页
//			SearchResponse response4MSTSFailed = es.client.prepareSearch("newgen*").setTypes("log")
//					.setQuery(qbIDMSTSFailed).setSize(100).addSort("@timestamp", SortOrder.DESC).get();
//
//			SearchHits hits4MSTSFailed = response4MSTSFailed.getHits();
//
//			file = new File(path + dirName + "/" + dirNameMSTS + "/", id + ".txt");
//			try {
//				fw = new FileWriter(file);
//				for (SearchHit hit : hits4MSTSFailed) {
//					System.out.println(hit.getSourceAsString());
//					fw.write(hit.getSourceAsString());
//					fw.write("\r\n");
//				}
//
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} finally {
//				fw.close();
//			}
//
//			System.out.println(
//					"---------------------------------MSTS Failed--------------------------------------------");
//		}
//
//		fwr.close();
//
//		System.out.println(
//				"RS Balance Total Hits Count: " + (hits4RME.totalHits + hits4EDENRED.totalHits + hits4MSTS.totalHits));
//		System.out.println("RS Balance RME Hits Count: " + hits4RME.totalHits);
//		System.out.println("RS Balance EDENRED Hits Count: " + hits4EDENRED.totalHits);
//		System.out.println("RS Balance MSTS Hits Count: " + hits4MSTS.totalHits);
//
//		System.out.println("SUCCESS Count: "
//				+ (hits4RMESuccess.totalHits + hits4EDENREDSuccess.totalHits + hits4MSTSSuccess.totalHits));
//		System.out.println("Total Failed Count: " + (idsRME.size() + idsEDENRED.size() + idsMSTS.size()));
//		System.out.println("RME Failed Count: " + idsRME.size());
//		System.out.println("EDENRED Failed Count: " + idsEDENRED.size());
//		System.out.println("MSTS Failed Count: " + idsMSTS.size());
//		System.out.println("Query " + hits4RME.totalHits + " Run Time: " + (endTime - startTime) / 1000 + "s");
//
//		es.close();
//
//	}
//
//}
