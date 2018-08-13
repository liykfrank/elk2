package elasticsearch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
	
	public static void main(String[] args) throws IOException {
		
//		String[] ids = TimeZone.getAvailableIDs();
//		for (String id:ids) 
//		 System.out.println(id+", ");
		
		String log = "{\"message\":\"[2018-04-26 02:05:19.459] INFO    org.mule.api.processor.LoggerMessageProcessor [[rs-balances-service].Balances-Service-SFTP-Flow.stage1.650]: 4527efc0-48f6-11e8-9229-060be056ede8: RS-Balances request has no EasyPay Vendor specified, so routing to Featurespace (RME)\",\"path\":\"/home/frank/elk6/NEWGEN_log/rs-balances-service/20180426_122122/5ad06ec89d095b0fb99baf8c-0.txt\",\"@timestamp\":\"2018-04-26T00:05:19.459Z\",\"@version\":\"1\",\"host\":\"0.0.0.0\",\"type\":\"rs-balances-service\"}\r\n";
		String code = "\"IATACode\":\"54471180013\",\"EventType\":\"Update\"";
		
//		\"iataCode\": \"67727354\",\r\n
//		String codeee = "iataCode\\":\"54471180013\"";
		
		String log2 = "{\"message\":\"[2018-05-27 07:29:15.305] INFO    org.mule.api.processor.LoggerMessageProcessor [[featurespace-system-service].featurespace-system-service-httpListenerConfig.worker.83]: a8a0a001-617f-11e8-8d81-064e51d24a64: POST-Agency RME Transformed Data: {\\n  \\\"eventId\\\": \\\"a8a0a001-617f-11e8-8d81-064e51d24a64\\\",\\n  \\\"eventType\\\": \\\"masterData\\\",\\n  \\\"schemaVersion\\\": 1,\\n  \\\"eventTime\\\": \\\"2018-05-27T07:29:15.304Z\\\",\\n  \\\"agencies\\\": [\\n    {\\n      \\\"FormOfPayment\\\": [\\n        \\n      ],\\n      \\\"LocationClass\\\": \\\"P\\\",\\n      \\\"RHCInfo\\\": {\\n        \\\"RiskStatus\\\": \\\"\\\",\\n        \\\"RHCCurrency\\\": \\\"EUR\\\"\\n      },\\n      \\\"RemittanceFrequency\\\": \\\"TWICE PER MONTH\\\",\\n      \\\"agentIATAStatus\\\": \\\"Approved\\\",\\n      \\\"agentLocationType\\\": \\\"HE\\\",\\n      \\\"agentName\\\": \\\"TUI OESTERREICH GMBH\\\",\\n      \\\"agentNumericCode\\\": \\\"06200876\\\",\\n      \\\"billingCity\\\": \\\"VIENNA\\\",\\n      \\\"billingCountry\\\": \\\"Austria\\\",\\n      \\\"billingPostalCode\\\": \\\"1190\\\",\\n      \\\"bspCountry\\\": \\\"AT\\\",\\n      \\\"countryISOCode\\\": \\\"AT\\\"\\n    }\\n  ]\\n}\",\"path\":\"/home/frank/elk6/NEWGEN_log/featurespace-system-service/20180528_123713/5b06ec3a36a2fc10b21446f1-1.txt\",\"@timestamp\":\"2018-05-27T05:29:15.305Z\",\"tags\":[\"multiline\"],\"@version\":\"1\",\"host\":\"0.0.0.0\",\"type\":\"featurespace-system-service\"}";
		// match event ID
		// String regx = "([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})";
		
		// 截取字符串中间的内容 ！！！
		String regx = "message\":\"(.*?)INFO";
		
		String regx2 = "POST-Agency RME Transformed Data:(.*?)\",\"path";
		
		Pattern pattern = Pattern.compile(regx2);
		
		Matcher match = pattern.matcher(log2);
				
		if(match.find()) {
			System.out.println(match.group(1));
		}
		
//		String date = result.substring(1, 24);
//		
//		System.out.println(date);
		
//		File file = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        String now = sdf.format(new Date());
//        String path = "c:/Users/wangjm@iata.org/Desktop/";
//        
//        file = new File(path + now);
//        
//        if (!file.exists()) {
//        	file.mkdir();
//        	System.out.println("Dir success");
//        } else {
//        	System.out.println("File already exist!");
//        }
//        
//        File file2 = new File(path + now + "/", "667776.txt");
////        try {
////			file2.createNewFile();
////		} catch (IOException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//        
//        
//        FileWriter fw = new FileWriter(file2);
//        
//        fw.write("777");
//        fw.write("\r\n");
//        fw.write("888");
        
//        FileOutputStream fOutputStream = null;
//        OutputStreamWriter writer = null;
//        try {
//			fOutputStream=new FileOutputStream(file2);
//			writer=new OutputStreamWriter(fOutputStream);
//			
//			writer.append("ajls;djf;alsjdlkfja;");
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
////			fOutputStream.close();
////			 .close();
//		}
//        fw.close();
        
//        System.out.println(file.getAbsolutePath());
		
		
//		
//		Date now = new Date();
//		String s = now.getTime() + "";
//		System.out.println(now.getTime());
        
	}

}
